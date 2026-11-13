import os
import tempfile
import unittest
from dataclasses import asdict

from window_join import (
    OP_DELETE,
    POLICY_ACCEPT,
    RETRACT,
    UPSERT,
    Event,
    WindowJoinOperator,
)


def ev(event_id, stream, key, event_time, seq, op="add", target_id=None):
    return Event(event_id=event_id, stream=stream, key=key,
                 event_time=event_time, ingestion_seq=seq,
                 op=op, target_id=target_id)


def pairs(op):
    return [(r.kind, r.window_start, r.left_id, r.right_id)
            for r in op.outputs()]


class BasicJoinTest(unittest.TestCase):
    def test_basic_two_stream_join(self):
        op = WindowJoinOperator(window_size=10)
        self.assertEqual(op.process(ev("l1", "L", "k", 1, 1)), [])
        out = op.process(ev("r1", "R", "k", 2, 2))
        self.assertEqual(len(out), 1)
        rec = out[0]
        self.assertEqual(rec.kind, UPSERT)
        self.assertEqual((rec.window_start, rec.window_end), (0, 10))
        self.assertEqual((rec.left_id, rec.right_id, rec.key), ("l1", "r1", "k"))
        self.assertFalse(rec.correction)

        # 不同 key 不关联
        self.assertEqual(op.process(ev("r2", "R", "other", 3, 3)), [])
        # 推进水位线关闭窗口
        op.process(ev("l2", "L", "k", 25, 4))
        op.process(ev("r3", "R", "k", 25, 5))
        self.assertTrue(op.is_closed(0))
        self.assertEqual(op.active_windows(), [20])

    def test_one_sided_window_produces_nothing(self):
        op = WindowJoinOperator(window_size=10)
        op.process(ev("l1", "L", "k", 1, 1))
        op.process(ev("l2", "L", "k", 2, 2))
        # 两路后续事件只推进水位线，key 不同不产生关联
        op.process(ev("r1", "R", "x", 50, 3))
        op.process(ev("l3", "L", "y", 50, 4))
        self.assertTrue(op.is_closed(0))
        self.assertEqual(op.outputs(), [])
        self.assertEqual(op.rejected, [])

    def test_watermark_is_min_of_two_streams(self):
        op = WindowJoinOperator(window_size=10)
        op.process(ev("l1", "L", "k", 25, 1))
        self.assertIsNone(op.watermark)  # 右流尚未上报，水位线不推进
        op.process(ev("r1", "R", "k", 3, 2))
        self.assertEqual(op.watermark, 3)  # 被慢的一路拖住
        op.process(ev("r2", "R", "k", 30, 3))
        self.assertEqual(op.watermark, 25)


class OutOfOrderAndWatermarkTest(unittest.TestCase):
    def test_out_of_order_within_lateness(self):
        op = WindowJoinOperator(window_size=10, allowed_lateness=10)
        op.process(ev("r1", "R", "k", 9, 1))
        op.process(ev("l1", "L", "k", 5, 2))
        op.process(ev("r2", "R", "k", 3, 3))
        op.process(ev("l2", "L", "k", 8, 4))
        result = set(pairs(op))
        expected = {
            (UPSERT, 0, "l1", "r1"), (UPSERT, 0, "l1", "r2"),
            (UPSERT, 0, "l2", "r1"), (UPSERT, 0, "l2", "r2"),
        }
        self.assertEqual(result, expected)

    def test_watermark_jump_closes_windows(self):
        op = WindowJoinOperator(window_size=10)
        op.process(ev("l1", "L", "k", 1, 1))
        op.process(ev("r1", "R", "k", 2, 2))
        op.process(ev("l2", "L", "k", 13, 3))
        self.assertEqual(op.active_windows(), [0, 10])
        # 水位线从 2 直接跳到 1000
        op.process(ev("r2", "R", "k", 1000, 4))
        op.process(ev("l3", "L", "k", 1000, 5))
        self.assertEqual(op.watermark, 1000)
        self.assertEqual(op.active_windows(), [1000])
        self.assertTrue(op.is_closed(0))
        self.assertTrue(op.is_closed(10))


class LateDataTest(unittest.TestCase):
    def _open_window_with_late_arrival(self):
        op = WindowJoinOperator(window_size=10)
        op.process(ev("l1", "L", "k", 5, 1))
        op.process(ev("r1", "R", "k", 9, 2))  # 水位线 = min(5, 9) = 5
        return op

    def test_late_upsert_in_active_window(self):
        op = self._open_window_with_late_arrival()
        out = op.process(ev("l2", "L", "k", 4, 3))  # 4 <= 水位线 5，迟到
        self.assertEqual(len(out), 1)
        rec = out[0]
        self.assertEqual(rec.kind, UPSERT)
        self.assertTrue(rec.correction)
        self.assertEqual((rec.left_id, rec.right_id), ("l2", "r1"))
        self.assertFalse(op.is_closed(0))

    def test_late_retract_in_active_window(self):
        op = self._open_window_with_late_arrival()
        op.process(ev("l2", "L", "k", 4, 3))
        out = op.process(ev("d1", "L", "k", 4, 4,
                            op=OP_DELETE, target_id="l2"))
        self.assertEqual(len(out), 1)
        rec = out[0]
        self.assertEqual(rec.kind, RETRACT)
        self.assertTrue(rec.correction)
        self.assertEqual((rec.left_id, rec.right_id), ("l2", "r1"))
        # 撤回后再次删除同一目标 -> 目标不存在，进入 rejected
        op.process(ev("d2", "L", "k", 4, 5, op=OP_DELETE, target_id="l2"))
        self.assertEqual(op.rejected[-1]["reason"], "delete-unknown-target")

    def test_closed_window_reject_policy(self):
        op = WindowJoinOperator(window_size=10, late_policy="reject")
        op.process(ev("l1", "L", "k", 1, 1))
        op.process(ev("r1", "R", "k", 1, 2))
        op.process(ev("l2", "L", "k", 20, 3))
        op.process(ev("r2", "R", "k", 20, 4))
        self.assertTrue(op.is_closed(0))
        out = op.process(ev("l3", "L", "k", 3, 5))  # 落入已关闭窗口
        self.assertEqual(out, [])
        self.assertEqual(len(op.rejected), 1)
        self.assertEqual(op.rejected[0]["reason"], "window-closed")
        self.assertEqual(op.rejected[0]["event"]["event_id"], "l3")
        # 已提交结果不受影响
        self.assertEqual(len(op.outputs()), 2)

    def test_closed_window_accept_policy_emits_correction(self):
        op = WindowJoinOperator(window_size=10, late_policy=POLICY_ACCEPT)
        op.process(ev("l1", "L", "k", 1, 1))
        op.process(ev("r1", "R", "k", 2, 2))
        op.process(ev("l2", "L", "k", 20, 3))
        op.process(ev("r2", "R", "k", 20, 4))
        self.assertTrue(op.is_closed(0))

        out = op.process(ev("l3", "L", "k", 3, 5))  # 已关闭窗口的迟到更正
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0].kind, UPSERT)
        self.assertTrue(out[0].correction)
        self.assertEqual((out[0].left_id, out[0].right_id), ("l3", "r1"))

        out = op.process(ev("d1", "L", "k", 3, 6,
                            op=OP_DELETE, target_id="l3"))
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0].kind, RETRACT)
        self.assertTrue(out[0].correction)


class OrderingTest(unittest.TestCase):
    def test_output_order_by_window_end_then_seq(self):
        op = WindowJoinOperator(window_size=10)
        op.process(ev("l1", "L", "k", 15, 1))   # 窗口 [10,20)
        op.process(ev("r1", "R", "k", 2, 2))    # 窗口 [0,10)
        op.process(ev("l2", "L", "k", 3, 3))    # 与 r1 关联，seq=3
        op.process(ev("r2", "R", "k", 16, 4))   # 与 l1 关联，seq=4
        ordered = op.outputs()
        keys = [(r.window_end, r.ingestion_seq, r.emission_idx)
                for r in ordered]
        self.assertEqual(keys, sorted(keys))
        self.assertEqual([(r.window_end, r.left_id, r.right_id)
                          for r in ordered],
                         [(10, "l2", "r1"), (20, "l1", "r2")])


class CheckpointTest(unittest.TestCase):
    EVENTS = [
        ev("l1", "L", "k", 1, 1),
        ev("r1", "R", "k", 2, 2),
        ev("l2", "L", "k", 4, 3),
        ev("r2", "R", "k", 12, 4),
        ev("l3", "L", "k", 25, 5),
        ev("r3", "R", "k", 25, 6),
        ev("l4", "L", "k", 3, 7),   # 窗口已关闭 -> rejected
        ev("r4", "R", "k", 40, 8),
    ]

    def test_checkpoint_restore_continues_identically(self):
        reference = WindowJoinOperator(window_size=10)
        for e in self.EVENTS:
            reference.process(e)

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "checkpoint.json")
            interrupted = WindowJoinOperator(window_size=10)
            for e in self.EVENTS[:4]:
                interrupted.process(e)
            interrupted.save_checkpoint(path)

            restored = WindowJoinOperator.restore(path)
            self.assertEqual(restored.watermark, interrupted.watermark)
            for e in self.EVENTS[4:]:
                restored.process(e)

        self.assertEqual([asdict(r) for r in restored.outputs()],
                         [asdict(r) for r in reference.outputs()])
        self.assertEqual(restored.rejected, reference.rejected)
        self.assertEqual(restored.active_windows(),
                         reference.active_windows())

    def test_restore_preserves_dedup_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "checkpoint.json")
            op = WindowJoinOperator(window_size=10)
            op.process(ev("l1", "L", "k", 1, 1))
            op.process(ev("r1", "R", "k", 2, 2))
            op.save_checkpoint(path)

            restored = WindowJoinOperator.restore(path)
            # 恢复后重复投递 checkpoint 前的事件，不重复影响结果
            self.assertEqual(restored.process(ev("l1", "L", "k", 1, 1)), [])
            self.assertEqual(restored.process(ev("r1", "R", "k", 2, 2)), [])
            self.assertEqual(len(restored.outputs()), 1)


class DuplicateDeliveryTest(unittest.TestCase):
    def test_duplicate_delivery_is_idempotent(self):
        op = WindowJoinOperator(window_size=10)
        event = ev("l1", "L", "k", 1, 1)
        op.process(event)
        self.assertEqual(op.process(event), [])
        op.process(ev("r1", "R", "k", 2, 2))
        op.process(ev("r1", "R", "k", 2, 2))  # 重复投递
        self.assertEqual(len(op.outputs()), 1)
        # 重复 delete 也不会重复撤回
        delete = ev("d1", "L", "k", 1, 3, op=OP_DELETE, target_id="l1")
        out = op.process(delete)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0].kind, RETRACT)
        self.assertEqual(op.process(delete), [])
        self.assertEqual(len(op.outputs()), 2)


if __name__ == "__main__":
    unittest.main()
