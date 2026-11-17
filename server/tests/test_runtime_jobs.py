"""ITEM-006: deterministic bounded scheduler and explicit lifecycle checks."""
import os
import subprocess
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from runtime_jobs import CapacityError, Job, Outcome, Scheduler


class SchedulerTests(unittest.TestCase):
    def test_reservations_are_atomic_and_lanes_independent(self):
        scheduler = Scheduler(lambda job: Outcome(), index_capacity=2, delivery_capacity=1)
        reservation = scheduler.reserve("index", 2)
        with self.assertRaises(CapacityError):
            scheduler.reserve("index", 1)
        delivery = scheduler.reserve("delivery", 1)
        self.assertEqual(scheduler.pending("index"), 2)
        reservation.cancel()
        reservation.cancel()
        self.assertEqual(scheduler.pending("index"), 0)
        delivery.cancel()

    def test_retry_clock_four_attempts_and_terminal_once(self):
        now, attempts, terminal = [0.0], [], []
        scheduler = Scheduler(lambda job: attempts.append(now[0]) or Outcome(False, True, "timeout"),
                              clock=lambda: now[0])
        scheduler.reserve("index", 1).submit([Job("a", {}, terminal=terminal.append)])
        for moment in (0, .5, 1, 2, 3, 6, 7, 100):
            now[0] = moment
            scheduler.run_ready()
        self.assertEqual(attempts, [0, 1, 3, 7])
        self.assertEqual(len(terminal), 1)
        self.assertEqual(terminal[0].error, "timeout")
        self.assertEqual(scheduler.pending("index"), 0)

    def test_deterministic_failure_and_deadline_do_not_retry(self):
        now, attempts, terminal = [2.0], [], []
        scheduler = Scheduler(lambda job: attempts.append(job.key) or Outcome(False, False, "mapping"),
                              clock=lambda: now[0])
        scheduler.reserve("index", 2).submit([
            Job("a", {}, terminal=terminal.append),
            Job("b", {}, deadline=1, terminal=terminal.append)])
        scheduler.run_ready()
        scheduler.run_ready()
        self.assertEqual(attempts, ["a"])
        self.assertEqual([o.error for o in terminal], ["mapping", "deadline"])

    def test_same_id_order_survives_retry_and_other_ids_progress(self):
        now, attempts = [0.0], []
        def execute(job):
            attempts.append(job.payload)
            return Outcome(False, True, "busy") if len(attempts) == 1 else Outcome()
        scheduler = Scheduler(execute, clock=lambda: now[0])
        scheduler.reserve("index", 3).submit([Job("a", 1), Job("a", 2), Job("b", 3)])
        scheduler.run_ready()
        scheduler.run_ready()
        scheduler.run_ready()
        self.assertEqual(attempts, [1, 3])
        now[0] = 1
        scheduler.run_ready()
        scheduler.run_ready()
        self.assertEqual(attempts, [1, 3, 1, 2])

    def test_shutdown_discards_queued_and_rejects_admission(self):
        terminal = []
        scheduler = Scheduler(lambda job: Outcome())
        scheduler.reserve("index", 1).submit([Job("a", {}, terminal=terminal.append)])
        scheduler.stop()
        self.assertEqual(scheduler.pending("index"), 0)
        self.assertEqual(terminal[0].error, "shutdown")
        with self.assertRaises(CapacityError):
            scheduler.reserve("index", 1)

    def test_two_workers_never_overlap_same_id(self):
        entered, release, other = threading.Event(), threading.Event(), threading.Event()
        active, peak, seen = set(), [0], []
        lock = threading.Lock()
        def execute(job):
            with lock:
                self.assertNotIn(job.key, active)
                active.add(job.key)
                peak[0] = max(peak[0], len(active))
            if job.payload == 1:
                entered.set()
                release.wait(2)
            if job.key == "b":
                other.set()
            with lock:
                active.remove(job.key)
                seen.append(job.payload)
            return Outcome()
        scheduler = Scheduler(execute)
        scheduler.reserve("index", 3).submit([Job("a", 1), Job("a", 2), Job("b", 3)])
        scheduler.start()
        try:
            self.assertTrue(entered.wait(1))
            self.assertTrue(other.wait(1))
        finally:
            release.set()
            scheduler.stop()
        self.assertLessEqual(peak[0], 2)
        self.assertIn(1, seen)

    def test_reservation_mismatch_does_not_consume_or_enqueue(self):
        scheduler = Scheduler(lambda job: Outcome(), index_capacity=2)
        reservation = scheduler.reserve("index", 2)
        with self.assertRaises(ValueError):
            reservation.submit([Job("a", {})])
        self.assertEqual(scheduler.pending("index"), 2)
        reservation.cancel()
        self.assertFalse(scheduler.run_ready())

    def test_partial_failures_retry_only_retryable_payload_unchanged(self):
        now, seen, terminal = [0.0], [], []
        def execute(job):
            seen.append((job.key, now[0], dict(job.payload)))
            return Outcome(False, job.key == "retry", "busy")
        scheduler = Scheduler(execute, clock=lambda: now[0])
        scheduler.reserve("index", 2).submit([
            Job("retry", {"version": 1}, terminal=terminal.append),
            Job("mapping", {"version": 2}, terminal=terminal.append)])
        for moment in (0, 1, 3, 7):
            now[0] = moment
            while scheduler.run_ready():
                pass
        self.assertEqual([when for key, when, _ in seen if key == "retry"], [0, 1, 3, 7])
        self.assertEqual([data for key, _, data in seen if key == "retry"], [{"version": 1}] * 4)
        self.assertEqual(sum(key == "mapping" for key, _, _ in seen), 1)
        self.assertEqual(len(terminal), 2)

    def test_stop_bounds_inflight_wait_and_does_not_publish_success(self):
        entered, release = threading.Event(), threading.Event()
        def execute(job):
            entered.set()
            release.wait(2)
            return Outcome()
        scheduler = Scheduler(execute)
        scheduler.reserve("index", 1).submit([Job("a", {})])
        scheduler.start()
        try:
            self.assertTrue(entered.wait(1))
            started = time.monotonic()
            self.assertFalse(scheduler.stop(timeout=.01))
            self.assertLess(time.monotonic() - started, 1)
            with self.assertRaises(CapacityError):
                scheduler.reserve("index", 1)
        finally:
            release.set()
            self.assertTrue(scheduler.stop(timeout=2))

    def test_server_import_creates_no_threads_or_network(self):
        code = """
import socket, threading
def denied(*a, **k):
    raise AssertionError('import side effect')
socket.create_connection = denied
threading.Thread.start = denied
import server
assert server.SEARCH_BACKEND is None
"""
        env = dict(os.environ, PRELOAD_FULL_SAMPLE="true")
        result = subprocess.run([sys.executable, "-c", code], cwd=os.path.dirname(
            os.path.dirname(__file__)), env=env, capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
