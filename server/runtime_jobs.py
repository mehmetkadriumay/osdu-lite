"""Volatile, explicitly started two-lane reservation and retry scheduler."""
import heapq
import threading
import time
from dataclasses import dataclass, field


class CapacityError(Exception):
    status = 429


@dataclass(frozen=True)
class Outcome:
    success: bool = True
    retryable: bool = False
    error: str = ""


@dataclass
class Job:
    key: str
    payload: object
    deadline: float = float("inf")
    terminal: object = None
    started: object = None
    fence: object = None
    attempts: int = field(default=0, init=False)


class Reservation:
    def __init__(self, scheduler, lane, count):
        self.scheduler, self.lane, self.count = scheduler, lane, count
        self.open = True

    def cancel(self):
        with self.scheduler.condition:
            if self.open:
                self.scheduler.used[self.lane] -= self.count
                self.open = False
                self.scheduler.reservations.discard(self)

    def submit(self, jobs, delay=0):
        jobs = list(jobs)
        scheduler = self.scheduler
        with scheduler.condition:
            if not self.open or scheduler.closed:
                raise CapacityError("scheduler_stopped")
            if len(jobs) != self.count or not 0 <= delay <= 5:
                raise ValueError("reservation size or delay mismatch")
            if len({id(job) for job in jobs}) != len(jobs):
                raise ValueError("duplicate job object")
            for job in jobs:
                scheduler.sequence += 1
                sequence = scheduler.sequence
                scheduler.order[self.lane].setdefault(job.key, []).append(sequence)
                heapq.heappush(scheduler.queues[self.lane],
                               (scheduler.clock() + delay, sequence, job))
            self.open = False
            scheduler.reservations.discard(self)
            scheduler.condition.notify_all()


class Scheduler:
    def __init__(self, index_execute, delivery_execute=None, *, clock=time.monotonic,
                 index_capacity=1000, delivery_capacity=1000, sweep=None):
        self.execute = {"index": index_execute, "delivery": delivery_execute}
        self.capacity = {"index": index_capacity, "delivery": delivery_capacity}
        self.used = dict.fromkeys(self.capacity, 0)
        self.queues = {lane: [] for lane in self.capacity}
        self.order = {lane: {} for lane in self.capacity}
        self.active = {lane: set() for lane in self.capacity}
        self.clock = clock
        self.condition = threading.Condition()
        self.sequence = 0
        self.threads = []
        self.reservations = set()
        self.closed = False
        self.sweep = sweep

    def pending(self, lane):
        with self.condition:
            return self.used[lane]

    def reserve(self, lane, count):
        with self.condition:
            if (self.closed or not isinstance(count, int) or count < 0
                    or count + self.used[lane] > self.capacity[lane]):
                raise CapacityError("job_capacity")
            self.used[lane] += count
            reservation = Reservation(self, lane, count)
            self.reservations.add(reservation)
            return reservation

    def _take(self, lane):
        queue, skipped, selected = self.queues[lane], [], None
        while queue and queue[0][0] <= self.clock():
            entry = heapq.heappop(queue)
            _, sequence, job = entry
            if job.key not in self.active[lane] and self.order[lane][job.key][0] == sequence:
                self.active[lane].add(job.key)
                selected = entry
                break
            skipped.append(entry)
        for entry in skipped:
            heapq.heappush(queue, entry)
        return selected

    @staticmethod
    def _notify(job, outcome):
        if job.terminal:
            job.terminal(outcome)

    def run_ready(self, lane="index"):
        with self.condition:
            if self.closed or self.execute[lane] is None:
                return False
            entry = self._take(lane)
        if entry is None:
            return False
        _, sequence, job = entry
        if self.clock() >= job.deadline:
            outcome = Outcome(False, False, "deadline")
        else:
            job.attempts += 1
            try:
                if job.started:
                    job.started()
                outcome = self.execute[lane](job)
                if not isinstance(outcome, Outcome):
                    raise TypeError("executor must return Outcome")
            except Exception:
                # Never retain exception text: it can contain a document or credential.
                outcome = Outcome(False, False, "worker_error")
        terminal = False
        with self.condition:
            self.active[lane].remove(job.key)
            if self.clock() >= job.deadline:
                outcome = Outcome(False, False, "deadline")
            retry = (not self.closed and not outcome.success and outcome.retryable
                     and job.attempts < 4 and self.clock() < job.deadline)
            if retry:
                delay = (1, 2, 4)[job.attempts - 1]
                heapq.heappush(self.queues[lane],
                               (min(self.clock() + delay, job.deadline), sequence, job))
            else:
                self.used[lane] -= 1
                order = self.order[lane][job.key]
                order.remove(sequence)
                if not order:
                    del self.order[lane][job.key]
                terminal = True
            self.condition.notify_all()
        if terminal:
            self._notify(job, outcome)
        return True

    def _worker(self, lane):
        while True:
            with self.condition:
                if self.closed:
                    return
            if not self.run_ready(lane):
                with self.condition:
                    if not self.closed:
                        self.condition.wait(.05)

    def sweep_deadlines(self):
        if self.sweep:
            self.sweep()
        expired = []
        with self.condition:
            for lane, queue in self.queues.items():
                retained = []
                for entry in queue:
                    _, sequence, job = entry
                    if self.clock() >= job.deadline:
                        self.used[lane] -= 1
                        order = self.order[lane][job.key]
                        order.remove(sequence)
                        if not order:
                            del self.order[lane][job.key]
                        expired.append(job)
                    else:
                        retained.append(entry)
                queue[:] = retained
                heapq.heapify(queue)
            self.condition.notify_all()
        for job in expired:
            self._notify(job, Outcome(False, False, "deadline"))

    def _sweeper(self):
        while True:
            with self.condition:
                if self.closed:
                    return
                self.condition.wait(.05)
                if self.closed:
                    return
            self.sweep_deadlines()

    def start(self):
        with self.condition:
            if self.closed:
                raise CapacityError("scheduler_stopped")
            if self.threads:
                return
            for lane, execute in self.execute.items():
                if execute is not None:
                    for number in range(2):
                        thread = threading.Thread(target=self._worker, args=(lane,),
                                                  name=f"osdu-{lane}-{number}", daemon=True)
                        self.threads.append(thread)
                        thread.start()
            thread = threading.Thread(target=self._sweeper, name="osdu-deadlines", daemon=True)
            self.threads.append(thread)
            thread.start()

    def stop(self, timeout=10):
        deadline = time.monotonic() + min(max(timeout, 0), 10)
        discarded = []
        with self.condition:
            self.closed = True
            for reservation in list(self.reservations):
                reservation.cancel()
            for lane, queue in self.queues.items():
                while queue:
                    _, sequence, job = heapq.heappop(queue)
                    self.used[lane] -= 1
                    order = self.order[lane][job.key]
                    order.remove(sequence)
                    if not order:
                        del self.order[lane][job.key]
                    discarded.append(job)
            self.condition.notify_all()
        for job in discarded:
            self._notify(job, Outcome(False, False, "shutdown"))
        for thread in self.threads:
            thread.join(max(0, deadline - time.monotonic()))
        return not any(thread.is_alive() for thread in self.threads)
