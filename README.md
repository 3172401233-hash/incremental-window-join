# incremental-window-join

Python 增量时间窗关联库，处理水位线、迟到事件、结果撤回和断点续算。
仅依赖标准库，核心实现在 `window_join.py`，测试在 `test_window_join.py`。

## 快速开始

```python
from window_join import Event, WindowJoinOperator

op = WindowJoinOperator(window_size=10, allowed_lateness=0,
                        late_policy="reject")
op.process(Event("l1", "L", key="k", event_time=1, ingestion_seq=1))
op.process(Event("r1", "R", key="k", event_time=2, ingestion_seq=2))
for record in op.outputs():
    print(record)
```

运行测试：`python -m unittest`

## 事件模型

- 每条事件携带 `event_id`、`stream`（`"L"` / `"R"`）、`key`、
  `event_time`（事件时间）、`ingestion_seq`（到达序号）和 `op`。
- `op="add"` 表示新增；`op="delete"` 配合 `target_id` 撤回一条已处理的
  add 事件，并为其产生过的关联结果发出 `retract`。
- `event_id` 全局唯一：重复投递同一条事件会被幂等忽略，不重复影响结果。

## 水位线规则

- 每一路流各自维护水位线：`已见最大 event_time - allowed_lateness`。
- 算子水位线取两路的最小值，且只增不减；两路都上报过数据后才开始推进
  （真实部署中空闲的一路应发送空闲标记，否则水位线停滞）。
- 窗口为左闭右开的滚动窗口 `[start, start + window_size)`；当
  `窗口结束时间 <= 算子水位线` 时窗口关闭，状态被提交。
- 水位线可以跳跃式前进：一次大事件时间会成批关闭所有满足条件的窗口。

## 迟到与更正规则

- 事件的 `event_time <= 当前水位线` 视为迟到。
- **迟到但窗口仍活动**：正常参与关联，输出标记 `correction=True` 的
  `upsert`（add）或 `retract`（delete），用于更正已流出的结果。
- **迟到且窗口已关闭**：由 `late_policy` 决定——
  - `"reject"`：事件写入 `rejected` 列表，已提交结果不受影响；
  - `"accept"`：算子保留已关闭窗口的状态，迟到事件照常产生
    `correction=True` 的 `upsert` / `retract` 更正。
- delete 指向不存在的目标时，事件写入 `rejected`
  （原因 `delete-unknown-target`）。

## 输出顺序

`outputs()` 返回的 changelog 按 `(窗口结束时间, ingestion_seq,
emission_idx)` 显式排序，其中 `emission_idx` 是单调递增的发射序号，
作为最终决胜项。排序不依赖字典遍历顺序，同一事件序列必然产生
完全一致的输出序列。

## Checkpoint 与恢复

- `save_checkpoint(path)` 把完整算子状态（水位线、活动/已关闭窗口、
  已见事件 id、已输出结果、rejected 列表、发射序号）写入 JSON 文件。
- `WindowJoinOperator.restore(path)` 恢复后继续处理后续事件，结果与
  无中断运行完全一致；恢复后对 checkpoint 之前的事件重复投递仍然幂等。
