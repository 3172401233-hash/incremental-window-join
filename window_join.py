"""增量时间窗关联离线模拟器（仅依赖标准库）。

模拟两路持续到达的事件流，按事件时间做固定（滚动）窗口内关联，
支持水位线推进、迟到数据更正（upsert / retract）、已关闭窗口的
迟到策略、确定性输出顺序以及基于 JSON 文件的 checkpoint 恢复。
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Dict, List, Optional

UPSERT = "upsert"
RETRACT = "retract"

STREAM_LEFT = "L"
STREAM_RIGHT = "R"
STREAMS = (STREAM_LEFT, STREAM_RIGHT)

OP_ADD = "add"
OP_DELETE = "delete"

POLICY_ACCEPT = "accept"
POLICY_REJECT = "reject"

STATE_VERSION = 1


@dataclass(frozen=True)
class Event:
    """一条输入事件。

    event_id 全局唯一，用于去重（重复投递幂等）。
    op 为 delete 时，target_id 指向需要撤回的 add 事件。
    """

    event_id: str
    stream: str
    key: str
    event_time: int
    ingestion_seq: int
    payload: object = None
    op: str = OP_ADD
    target_id: Optional[str] = None


@dataclass
class OutputRecord:
    """一条输出记录（changelog）。"""

    kind: str  # UPSERT 或 RETRACT
    window_start: int
    window_end: int
    key: str
    left_id: str
    right_id: str
    ingestion_seq: int  # 触发该输出的事件序号
    emission_idx: int  # 单调递增发射序号，排序的最终决胜项
    correction: bool = False  # 是否为迟到更正


class WindowJoinOperator:
    """两路事件流的固定窗口内关联算子。

    - window_size: 滚动窗口长度，窗口为 [start, start + window_size)。
    - allowed_lateness: 水位线 = 已见最大事件时间 - allowed_lateness。
    - late_policy: 已关闭窗口再收到数据时的策略，
      POLICY_REJECT 写入 rejected，POLICY_ACCEPT 保留状态并产生更正。
    """

    def __init__(self, window_size: int, allowed_lateness: int = 0,
                 late_policy: str = POLICY_REJECT):
        if window_size <= 0:
            raise ValueError("window_size must be positive")
        if allowed_lateness < 0:
            raise ValueError("allowed_lateness must be >= 0")
        if late_policy not in (POLICY_ACCEPT, POLICY_REJECT):
            raise ValueError("unknown late_policy: %r" % late_policy)
        self.window_size = window_size
        self.allowed_lateness = allowed_lateness
        self.late_policy = late_policy

        self._max_event_time: Dict[str, Optional[int]] = {
            STREAM_LEFT: None, STREAM_RIGHT: None}
        self._watermark: Optional[int] = None
        # 活动窗口：window_start -> {"L": {event_id: Event}, "R": {...}}
        self._windows: Dict[int, Dict[str, Dict[str, Event]]] = {}
        self._closed: set = set()
        # accept 策略下保留已关闭窗口的状态，用于迟到更正
        self._closed_windows: Dict[int, Dict[str, Dict[str, Event]]] = {}
        self._seen_ids: set = set()
        self._outputs: List[OutputRecord] = []
        self._rejected: List[dict] = []
        self._emission_idx = 0

    # ------------------------------------------------------------------
    # 水位线
    # ------------------------------------------------------------------
    @property
    def watermark(self) -> Optional[int]:
        return self._watermark

    def _recompute_watermark(self) -> None:
        # 算子水位线取两路各自水位线的最小值；两路都上报过数据后才开始推进
        # （真实部署中空闲的一路应发送空闲标记，否则水位线会停滞）。
        per_stream = []
        for stream in STREAMS:
            max_et = self._max_event_time[stream]
            if max_et is None:
                return
            per_stream.append(max_et - self.allowed_lateness)
        wm = min(per_stream)
        if self._watermark is None or wm > self._watermark:
            self._watermark = wm

    # ------------------------------------------------------------------
    # 主入口
    # ------------------------------------------------------------------
    def process(self, event: Event) -> List[OutputRecord]:
        """处理一条事件，返回本次触发的输出记录。"""
        if event.stream not in STREAMS:
            raise ValueError("unknown stream: %r" % event.stream)
        if event.op not in (OP_ADD, OP_DELETE):
            raise ValueError("unknown op: %r" % event.op)
        if event.op == OP_DELETE and not event.target_id:
            raise ValueError("delete event requires target_id")

        # 重复投递幂等：同一条事件不重复影响结果。
        if event.event_id in self._seen_ids:
            return []
        self._seen_ids.add(event.event_id)

        max_et = self._max_event_time[event.stream]
        if max_et is None or event.event_time > max_et:
            self._max_event_time[event.stream] = event.event_time
        self._recompute_watermark()

        window_start = (event.event_time // self.window_size) * self.window_size
        late = (self._watermark is not None
                and event.event_time <= self._watermark)

        if window_start in self._closed:
            if self.late_policy == POLICY_ACCEPT:
                state = self._closed_windows[window_start]
                emitted = self._apply(state, event, window_start,
                                      correction=True)
            else:
                self._rejected.append({
                    "event": asdict(event),
                    "reason": "window-closed",
                })
                emitted = []
        else:
            state = self._windows.setdefault(
                window_start, {STREAM_LEFT: {}, STREAM_RIGHT: {}})
            emitted = self._apply(state, event, window_start,
                                  correction=late)

        self._close_windows()
        return emitted

    # ------------------------------------------------------------------
    # 关联与窗口生命周期
    # ------------------------------------------------------------------
    def _apply(self, state: Dict[str, Dict[str, Event]], event: Event,
               window_start: int, correction: bool) -> List[OutputRecord]:
        own = state[event.stream]
        other_stream = (STREAM_RIGHT if event.stream == STREAM_LEFT
                        else STREAM_LEFT)
        other = state[other_stream]
        emitted: List[OutputRecord] = []

        if event.op == OP_ADD:
            own[event.event_id] = event
            peers = sorted(
                (p for p in other.values() if p.key == event.key),
                key=lambda p: (p.ingestion_seq, p.event_id))
            for peer in peers:
                left_id = (event.event_id if event.stream == STREAM_LEFT
                           else peer.event_id)
                right_id = (peer.event_id if event.stream == STREAM_LEFT
                            else event.event_id)
                emitted.append(self._emit(
                    UPSERT, window_start, event.key, left_id, right_id,
                    event.ingestion_seq, correction))
        else:
            target = own.pop(event.target_id, None)
            if target is None:
                self._rejected.append({
                    "event": asdict(event),
                    "reason": "delete-unknown-target",
                })
                return emitted
            peers = sorted(
                (p for p in other.values() if p.key == target.key),
                key=lambda p: (p.ingestion_seq, p.event_id))
            for peer in peers:
                left_id = (target.event_id if target.stream == STREAM_LEFT
                           else peer.event_id)
                right_id = (peer.event_id if target.stream == STREAM_LEFT
                            else target.event_id)
                emitted.append(self._emit(
                    RETRACT, window_start, target.key, left_id, right_id,
                    event.ingestion_seq, correction))
        return emitted

    def _emit(self, kind: str, window_start: int, key: str,
              left_id: str, right_id: str, ingestion_seq: int,
              correction: bool) -> OutputRecord:
        record = OutputRecord(
            kind=kind,
            window_start=window_start,
            window_end=window_start + self.window_size,
            key=key,
            left_id=left_id,
            right_id=right_id,
            ingestion_seq=ingestion_seq,
            emission_idx=self._emission_idx,
            correction=correction,
        )
        self._emission_idx += 1
        self._outputs.append(record)
        return record

    def _close_windows(self) -> None:
        if self._watermark is None:
            return
        for window_start in sorted(self._windows):
            if window_start + self.window_size <= self._watermark:
                state = self._windows.pop(window_start)
                self._closed.add(window_start)
                if self.late_policy == POLICY_ACCEPT:
                    self._closed_windows[window_start] = state

    # ------------------------------------------------------------------
    # 输出访问
    # ------------------------------------------------------------------
    def outputs(self) -> List[OutputRecord]:
        """按 (窗口结束时间, 事件序号, 发射序号) 排序的确定性输出。"""
        return sorted(
            self._outputs,
            key=lambda r: (r.window_end, r.ingestion_seq, r.emission_idx))

    @property
    def rejected(self) -> List[dict]:
        return list(self._rejected)

    def is_closed(self, window_start: int) -> bool:
        return window_start in self._closed

    def active_windows(self) -> List[int]:
        return sorted(self._windows)

    # ------------------------------------------------------------------
    # Checkpoint
    # ------------------------------------------------------------------
    def save_checkpoint(self, path: str) -> None:
        """把算子完整状态写入 JSON 文件，供中断后恢复。"""
        state = {
            "version": STATE_VERSION,
            "config": {
                "window_size": self.window_size,
                "allowed_lateness": self.allowed_lateness,
                "late_policy": self.late_policy,
            },
            "max_event_time": self._max_event_time,
            "watermark": self._watermark,
            "emission_idx": self._emission_idx,
            "seen_ids": sorted(self._seen_ids),
            "windows": self._serialize_windows(self._windows),
            "closed": sorted(self._closed),
            "closed_windows": self._serialize_windows(self._closed_windows),
            "outputs": [asdict(r) for r in self._outputs],
            "rejected": self._rejected,
        }
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(state, fh, ensure_ascii=False, indent=2)

    @classmethod
    def restore(cls, path: str) -> "WindowJoinOperator":
        """从 checkpoint 文件恢复算子，可继续处理后续事件。"""
        with open(path, "r", encoding="utf-8") as fh:
            state = json.load(fh)
        if state["version"] != STATE_VERSION:
            raise ValueError("unsupported checkpoint version: %r"
                             % state["version"])
        config = state["config"]
        op = cls(config["window_size"], config["allowed_lateness"],
                 config["late_policy"])
        op._max_event_time = {
            stream: state["max_event_time"].get(stream) for stream in STREAMS}
        op._watermark = state["watermark"]
        op._emission_idx = state["emission_idx"]
        op._seen_ids = set(state["seen_ids"])
        op._windows = cls._deserialize_windows(state["windows"])
        op._closed = set(state["closed"])
        op._closed_windows = cls._deserialize_windows(state["closed_windows"])
        op._outputs = [OutputRecord(**r) for r in state["outputs"]]
        op._rejected = list(state["rejected"])
        return op

    @staticmethod
    def _serialize_windows(windows) -> dict:
        return {
            str(ws): {
                stream: [asdict(e) for e in
                         sorted(buckets.values(),
                                key=lambda e: (e.ingestion_seq, e.event_id))]
                for stream, buckets in streams.items()
            }
            for ws, streams in windows.items()
        }

    @staticmethod
    def _deserialize_windows(data) -> dict:
        return {
            int(ws): {
                stream: {e["event_id"]: Event(**e) for e in events}
                for stream, events in streams.items()
            }
            for ws, streams in data.items()
        }
