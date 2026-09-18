"""The asyncio scheduler: queue in, leases out, retries in the middle.

One worker coroutine per slot, all pulling from one queue. The worker does five
things and nothing else:

    1. take a spec off the queue
    2. lease a device that matches its selector
    3. run the target under a hard deadline
    4. classify the outcome
    5. release the lease, and requeue if the work item is still alive

Why a shared queue and not a queue per device: a per-device queue commits a task
to one phone at submit time, and if that phone dies the task dies with it. With
a shared queue, "which device" is decided at the last possible moment, so a
retry naturally lands somewhere else.

Why the deadline lives here and not in the target: a target that has wedged
cannot be trusted to time itself out. `asyncio.wait_for` cancels from the
outside, which is the only enforcement that survives a target that is stuck in a
blocking call it never expected to block in.
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from typing import Any, Optional

from core.device import DeviceRegistry, DeviceState, Lease
from core.health import HealthMonitor
from core.session import SessionError
from core.task import (
    Attempt,
    FatalError,
    RetryableError,
    Target,
    TaskContext,
    TaskRecord,
    TaskSpec,
    TaskState,
    ProgressSink,
)
from obs.log import get_logger, log_context, new_correlation_id

log = get_logger("core.scheduler")


@dataclass
class SchedulerPolicy:
    workers: int = 4
    lease_timeout_s: float = 60.0      # how long a task waits for a free device
    retry_base_s: float = 2.0
    retry_max_s: float = 30.0
    retry_jitter: float = 0.3          # +/- fraction, so retries do not synchronise


class Scheduler:
    def __init__(
        self,
        registry: DeviceRegistry,
        targets: dict[str, Target],
        *,
        policy: Optional[SchedulerPolicy] = None,
        sink: Optional[ProgressSink] = None,
        health: Optional[HealthMonitor] = None,
    ) -> None:
        self._registry = registry
        self._targets = targets
        self._policy = policy or SchedulerPolicy()
        self._sink = sink
        self._health = health
        self._queue: asyncio.Queue[Optional[str]] = asyncio.Queue()
        self._records: dict[str, TaskRecord] = {}
        self._workers: list[asyncio.Task[None]] = []
        self._running = False
        self._idle = asyncio.Event()
        self._idle.set()
        self._inflight = 0
        # Tasks sleeping out a retry backoff are neither queued nor in flight.
        # Without counting them, `drain()` reports the fleet idle while a retry
        # is still pending and a caller walks away from unfinished work.
        self._pending_retries = 0

    # ---------- public surface ----------

    def submit(self, spec: TaskSpec) -> TaskRecord:
        if spec.kind not in self._targets:
            record = TaskRecord(
                spec=spec,
                correlation_id=new_correlation_id(),
                state=TaskState.REJECTED,
                error=f"no target registered for kind={spec.kind!r}",
            )
            self._records[spec.id] = record
            log.warning("task.rejected", task_id=spec.id, kind=spec.kind)
            return record

        record = TaskRecord(spec=spec, correlation_id=new_correlation_id())
        self._records[spec.id] = record
        self._idle.clear()
        self._queue.put_nowait(spec.id)
        log.info(
            "task.submitted",
            task_id=spec.id,
            kind=spec.kind,
            correlation_id=record.correlation_id,
            queue_depth=self._queue.qsize(),
        )
        return record

    def get(self, task_id: str) -> Optional[TaskRecord]:
        return self._records.get(task_id)

    def records(self) -> list[TaskRecord]:
        return sorted(self._records.values(), key=lambda r: r.submitted_at)

    def stats(self) -> dict[str, Any]:
        by_state: dict[str, int] = {}
        for record in self._records.values():
            by_state[record.state.value] = by_state.get(record.state.value, 0) + 1
        return {
            "queue_depth": self._queue.qsize(),
            "inflight": self._inflight,
            "pending_retries": self._pending_retries,
            "workers": len(self._workers),
            "by_state": by_state,
        }

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        for index in range(self._policy.workers):
            self._workers.append(
                asyncio.create_task(self._worker(index), name=f"worker-{index}")
            )
        log.info("scheduler.started", workers=self._policy.workers)

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        for _ in self._workers:
            self._queue.put_nowait(None)  # poison pill per worker
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()
        for target in self._targets.values():
            await target.close()
        log.info("scheduler.stopped")

    async def drain(self, timeout: Optional[float] = None) -> bool:
        """Wait until nothing is queued or running. True if it went quiet."""
        try:
            await asyncio.wait_for(self._idle.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    # ---------- worker ----------

    async def _worker(self, index: int) -> None:
        while True:
            task_id = await self._queue.get()
            if task_id is None:
                self._queue.task_done()
                return
            self._inflight += 1
            try:
                record = self._records.get(task_id)
                if record is None or record.state in (
                    TaskState.SUCCEEDED,
                    TaskState.REJECTED,
                ):
                    continue
                with log_context(
                    correlation_id=record.correlation_id,
                    task_id=task_id,
                    worker=index,
                ):
                    await self._run_once(record)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - a worker must never die
                log.error(
                    "worker.crashed", exc_info=True, worker=index, reason=str(exc)
                )
            finally:
                self._inflight -= 1
                self._queue.task_done()
                self._settle_idle()

    def _settle_idle(self) -> None:
        if self._queue.empty() and self._inflight == 0 and self._pending_retries == 0:
            self._idle.set()

    async def _run_once(self, record: TaskRecord) -> None:
        spec = record.spec
        target = self._targets[spec.kind]

        # Before blocking on a lease, check whether any device could ever match.
        # Waiting 60s for a selector nothing satisfies is a slow way to report a
        # typo in a tag name.
        if not self._registry.could_ever_match(spec.selector):
            self._finish(
                record,
                TaskState.ABANDONED,
                error="no device in the fleet matches this selector",
            )
            await self._emit_state(record)
            return

        lease = await self._registry.acquire(
            spec.selector, timeout=self._policy.lease_timeout_s
        )
        if lease is None:
            log.info("task.lease_timeout", task_id=spec.id)
            await self._requeue(record, reason="no device available")
            return

        attempt = Attempt(number=record.attempt_count + 1, device_id=lease.device_id)
        record.attempts.append(attempt)
        record.state = TaskState.RUNNING

        with log_context(device_id=lease.device_id, attempt=attempt.number):
            ctx = TaskContext(
                task_id=spec.id,
                correlation_id=record.correlation_id,
                attempt=attempt.number,
                log=get_logger(f"target.{spec.kind}"),
                deadline=time.monotonic() + spec.timeout_s,
                device_id=lease.device_id,
                sink=self._sink,
            )
            await self._emit_state(record)
            log.info("task.started", kind=spec.kind, timeout_s=spec.timeout_s)

            blames_device = False
            try:
                result = await asyncio.wait_for(
                    target.execute(spec, lease, ctx), timeout=spec.timeout_s
                )
                attempt.ended_at = time.time()
                self._finish(record, TaskState.SUCCEEDED, result=result)
                log.info(
                    "task.succeeded",
                    duration_s=round(attempt.ended_at - attempt.started_at, 2),
                )

            except FatalError as exc:
                attempt.ended_at = time.time()
                attempt.error = str(exc)
                self._finish(record, TaskState.REJECTED, error=str(exc))
                log.warning("task.rejected", reason=str(exc))

            except RetryableError as exc:
                attempt.ended_at = time.time()
                attempt.error = str(exc)
                blames_device = exc.blames_device
                log.warning(
                    "task.attempt_failed",
                    reason=str(exc),
                    blames_device=blames_device,
                )
                await self._after_failure(record, lease, blames_device, str(exc))

            except SessionError as exc:
                # Never getting a session is always the device's fault, not the
                # script's -- the script never ran.
                attempt.ended_at = time.time()
                attempt.error = str(exc)
                blames_device = True
                log.warning("task.session_failed", reason=str(exc))
                await self._after_failure(record, lease, True, str(exc))

            except asyncio.TimeoutError:
                attempt.ended_at = time.time()
                attempt.error = f"timed out after {spec.timeout_s}s"
                blames_device = True
                log.warning("task.timed_out", timeout_s=spec.timeout_s)
                await self._after_failure(record, lease, True, attempt.error)

            except asyncio.CancelledError:
                attempt.ended_at = time.time()
                attempt.error = "cancelled"
                await self._registry.release(lease)
                raise

            except Exception as exc:  # noqa: BLE001 - unclassified == suspicious
                attempt.ended_at = time.time()
                attempt.error = repr(exc)
                blames_device = True
                log.error("task.crashed", exc_info=True, reason=repr(exc))
                await self._after_failure(record, lease, True, repr(exc))

            finally:
                await self._registry.release(lease)
                # PENDING was already announced by `_requeue`, which knows the
                # backoff; re-announcing here would emit the same transition
                # twice to every subscriber.
                if record.state is not TaskState.PENDING:
                    await self._emit_state(record)

    async def _after_failure(
        self, record: TaskRecord, lease: Lease, blames_device: bool, reason: str
    ) -> None:
        if blames_device and self._health is not None:
            device = self._registry.get(lease.device_id)
            if device is not None and device.state == DeviceState.ONLINE:
                # Awaited, and awaited *before* the lease is released. Firing
                # this off as a background task leaves a window in which the
                # next task in the queue leases the phone that just failed --
                # which is exactly the bug the first demo run exposed. The
                # marking is cheap; the recovery behind it is what runs in the
                # background, inside the health monitor.
                await self._health.quarantine(lease.device_id, reason=reason)
        await self._requeue(record, reason=reason)

    async def _requeue(self, record: TaskRecord, reason: str) -> None:
        spec = record.spec
        if record.attempt_count >= spec.max_attempts:
            self._finish(
                record,
                TaskState.FAILED,
                error=f"{reason} (after {record.attempt_count} attempts)",
            )
            log.warning("task.failed", attempts=record.attempt_count, reason=reason)
            await self._emit_state(record)
            return

        delay = self._backoff(record.attempt_count)
        record.state = TaskState.PENDING
        log.info(
            "task.requeued",
            attempt=record.attempt_count,
            max_attempts=spec.max_attempts,
            retry_in_s=round(delay, 2),
            reason=reason,
        )
        await self._emit_state(record)
        # Re-enqueue after the backoff without holding a worker slot: the delay
        # is dead time for this task, not for the fleet. The counter is bumped
        # synchronously, before the task is created, so there is no instant in
        # which this work item is invisible to `drain()`.
        self._pending_retries += 1
        self._idle.clear()
        asyncio.create_task(self._delayed_put(spec.id, delay), name=f"retry-{spec.id}")

    async def _delayed_put(self, task_id: str, delay: float) -> None:
        try:
            await asyncio.sleep(delay)
            await self._queue.put(task_id)
        finally:
            self._pending_retries -= 1
            self._settle_idle()

    def _backoff(self, attempt: int) -> float:
        raw = min(self._policy.retry_max_s, self._policy.retry_base_s * (2 ** attempt))
        jitter = raw * self._policy.retry_jitter
        return max(0.1, raw + random.uniform(-jitter, jitter))

    def _finish(
        self,
        record: TaskRecord,
        state: TaskState,
        *,
        result: Optional[dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> None:
        record.state = state
        record.result = result
        record.error = error

    async def _emit_state(self, record: TaskRecord) -> None:
        if self._sink is None:
            return
        await self._sink(
            {
                "type": "task.state",
                "ts": time.time(),
                "task_id": record.spec.id,
                "correlation_id": record.correlation_id,
                "state": record.state.value,
                "attempt": record.attempt_count,
                "device_id": (
                    record.attempts[-1].device_id if record.attempts else None
                ),
                "error": record.error,
            }
        )
