"""Health monitoring, quarantine, and the recovery circuit breaker.

This is the module that decides a device is sick, tries to fix it, and -- the
part that matters most -- decides to stop trying.

Three states, and the transitions between them are the whole design:

    ONLINE  --(N consecutive failed probes)-->  QUARANTINED
    QUARANTINED --(recovery succeeded)-->       ONLINE
    QUARANTINED --(too many recoveries)-->      RETIRED

RETIRED is deliberate. A device that needs recovering every few minutes is not
recovering; it is oscillating, and an orchestrator that keeps handing it work is
choosing to fail every task that lands on it. Retirement costs one device and
saves the queue, and it is the state a human should be paged about.

The probe is a real adb round trip with a short deadline, not a socket check. A
tunnelled phone whose SSH forward is up but whose adb daemon has wedged will
accept a TCP connection and then answer nothing -- a black hole. Only a command
that must come back catches that.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional

from core.device import Adb, AdbNotAvailable, Device, DeviceRegistry, DeviceState
from core.session import SessionManager
from obs.log import get_logger, log_context, new_correlation_id

log = get_logger("core.health")

EventSink = Callable[[dict[str, Any]], Awaitable[None]]

# Transports that adb can actually reach. Anything else (a browser slot) is
# scheduled like a device but is not probed or reconnected like one.
ADB_TRANSPORTS = frozenset({"usb", "tcp", "tunnel"})


@dataclass
class HealthPolicy:
    interval_s: float = 15.0          # how often each device is probed
    probe_timeout_s: float = 5.0      # a probe slower than this counts as failed
    failures_to_quarantine: int = 2   # consecutive failures before pulling it
    recovery_attempts: int = 2        # tries per quarantine episode
    retire_after_recoveries: int = 5  # lifetime recoveries before giving up
    recovery_window_s: float = 900.0  # recoveries older than this stop counting


@dataclass
class DeviceHealth:
    device_id: str
    consecutive_failures: int = 0
    last_ok_at: Optional[float] = None
    last_probe_ms: Optional[float] = None
    recovery_times: list[float] = field(default_factory=list)

    def recent_recoveries(self, window_s: float) -> int:
        cutoff = time.monotonic() - window_s
        self.recovery_times = [t for t in self.recovery_times if t >= cutoff]
        return len(self.recovery_times)

    def to_dict(self) -> dict[str, Any]:
        return {
            "device_id": self.device_id,
            "consecutive_failures": self.consecutive_failures,
            "last_ok_at": self.last_ok_at,
            "last_probe_ms": self.last_probe_ms,
            "recoveries_in_window": len(self.recovery_times),
        }


class HealthMonitor:
    """Background supervisor for the device pool."""

    def __init__(
        self,
        registry: DeviceRegistry,
        adb: Adb,
        sessions: SessionManager,
        *,
        policy: Optional[HealthPolicy] = None,
        sink: Optional[EventSink] = None,
        probe: Optional[Callable[[Device], Awaitable[bool]]] = None,
    ) -> None:
        self._registry = registry
        self._adb = adb
        self._sessions = sessions
        self._policy = policy or HealthPolicy()
        self._sink = sink
        # Injectable so the demo and the tests can drive failures deterministically
        # instead of unplugging a real cable.
        self._probe_override = probe
        self._health: dict[str, DeviceHealth] = {}
        self._task: Optional[asyncio.Task[None]] = None
        self._recoveries: dict[str, asyncio.Task[None]] = {}
        self._stopping = asyncio.Event()

    def health_for(self, device_id: str) -> DeviceHealth:
        state = self._health.get(device_id)
        if state is None:
            state = DeviceHealth(device_id=device_id)
            self._health[device_id] = state
        return state

    def snapshot(self) -> list[dict[str, Any]]:
        return [h.to_dict() for h in self._health.values()]

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stopping.clear()
        self._task = asyncio.create_task(self._loop(), name="health-monitor")
        log.info(
            "health.started",
            interval_s=self._policy.interval_s,
            probe_timeout_s=self._policy.probe_timeout_s,
        )

    async def stop(self) -> None:
        self._stopping.set()
        for task in (self._task, *self._recoveries.values()):
            if task is None:
                continue
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        self._recoveries.clear()
        self._task = None
        log.info("health.stopped")

    async def _loop(self) -> None:
        while not self._stopping.is_set():
            try:
                await self._registry.refresh()
                await self._sweep()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - the monitor must not die
                # If this loop dies the fleet degrades silently, which is worse
                # than any single failure it was trying to handle.
                log.error("health.sweep_failed", exc_info=True, reason=str(exc))
            try:
                await asyncio.wait_for(
                    self._stopping.wait(), timeout=self._policy.interval_s
                )
            except asyncio.TimeoutError:
                pass

    async def _sweep(self) -> None:
        devices = [
            d
            for d in self._registry.all()
            if d.state in (DeviceState.ONLINE, DeviceState.QUARANTINED)
        ]
        if not devices:
            return
        # Probes run concurrently: with 30 phones and a 5s timeout, serial
        # probing would take longer than the interval and the monitor would
        # never catch up.
        await asyncio.gather(
            *(self._check_one(d) for d in devices), return_exceptions=True
        )

    async def _check_one(self, device: Device) -> None:
        # A leased device is being exercised by a task right now; probing it
        # concurrently competes for the same adb transport and produces false
        # negatives. The task's own timeout covers that window.
        if self._registry.is_leased(device.id):
            return

        health = self.health_for(device.id)
        started = time.monotonic()
        ok = await self._run_probe(device)
        health.last_probe_ms = round((time.monotonic() - started) * 1000, 1)

        if ok:
            health.last_ok_at = time.time()
            if health.consecutive_failures:
                log.info(
                    "health.recovered_on_probe",
                    device_id=device.id,
                    after_failures=health.consecutive_failures,
                )
            health.consecutive_failures = 0
            if device.state == DeviceState.QUARANTINED:
                await self._restore(device, health, reason="probe recovered")
            return

        health.consecutive_failures += 1
        log.warning(
            "health.probe_failed",
            device_id=device.id,
            consecutive=health.consecutive_failures,
            probe_ms=health.last_probe_ms,
        )
        if (
            device.state == DeviceState.ONLINE
            and health.consecutive_failures >= self._policy.failures_to_quarantine
        ):
            await self.quarantine(device.id, reason="probe failed")
        elif device.state == DeviceState.QUARANTINED:
            self._spawn_recovery(device, health)

    async def _run_probe(self, device: Device) -> bool:
        if self._probe_override is not None:
            return await self._probe_override(device)
        if device.transport not in ADB_TRANSPORTS:
            # A browser slot is a device to the scheduler, but it has no adb
            # path and nothing to ping. Probing it with adb fails every sweep
            # and quarantines a perfectly good slot -- which is exactly what the
            # first served run did to every browser. A virtual slot proves
            # itself when a session opens on it; that failure is already handled
            # by the scheduler, so the monitor has no separate opinion here.
            return True
        try:
            return await self._adb.ping(device.id, timeout=self._policy.probe_timeout_s)
        except AdbNotAvailable:
            # No adb on this host: the monitor has no opinion, and refusing to
            # have an opinion is better than quarantining the whole fleet.
            return True

    async def quarantine(self, device_id: str, reason: str) -> None:
        """Pull a device out of the pool now; recover it in the background.

        Split in two on purpose. The caller is usually a scheduler worker that
        has just had a task fail on this device, and it must not go back to the
        queue until the device is unassignable -- otherwise the next task in
        line leases the same broken phone in the window before the state
        change lands. Marking is awaited; recovery, which can take tens of
        seconds, is not.
        """
        await self._mark_quarantined(device_id, reason)
        device = self._registry.get(device_id)
        if device is None:
            return
        health = self.health_for(device_id)
        self._spawn_recovery(device, health)

    async def _mark_quarantined(self, device_id: str, reason: str) -> None:
        await self._registry.set_state(device_id, DeviceState.QUARANTINED, note=reason)
        # Any cached session pointing at this device is now a liability.
        await self._sessions.invalidate(device_id)
        await self._emit(
            {"type": "device.quarantined", "device_id": device_id, "reason": reason}
        )

    def _spawn_recovery(self, device: Device, health: DeviceHealth) -> None:
        existing = self._recoveries.get(device.id)
        if existing is not None and not existing.done():
            return  # one recovery per device at a time
        task = asyncio.create_task(
            self._attempt_recovery(device, health), name=f"recover-{device.id}"
        )
        self._recoveries[device.id] = task
        task.add_done_callback(lambda _t: self._recoveries.pop(device.id, None))

    async def _attempt_recovery(self, device: Device, health: DeviceHealth) -> None:
        if health.recent_recoveries(self._policy.recovery_window_s) >= (
            self._policy.retire_after_recoveries
        ):
            await self._retire(device, health)
            return

        if device.transport not in ADB_TRANSPORTS:
            # Nothing to reconnect. Restore it and let the next session attempt
            # decide; a virtual slot has no out-of-band repair.
            await self._restore(device, health, reason="virtual slot, no adb repair")
            return

        correlation_id = new_correlation_id("heal")
        with log_context(correlation_id=correlation_id, device_id=device.id):
            for attempt in range(1, self._policy.recovery_attempts + 1):
                log.info(
                    "health.recovery_attempt",
                    attempt=attempt,
                    of=self._policy.recovery_attempts,
                    transport=device.transport,
                )
                try:
                    reconnected = await self._adb.reconnect(device.id)
                except AdbNotAvailable:
                    reconnected = False
                if reconnected and await self._run_probe(device):
                    health.recovery_times.append(time.monotonic())
                    device.recoveries += 1
                    health.consecutive_failures = 0
                    await self._restore(device, health, reason="adb reconnect")
                    return
                await asyncio.sleep(min(2.0 * attempt, 10.0))

            log.warning("health.recovery_exhausted", device_id=device.id)
            await self._emit(
                {"type": "device.recovery_failed", "device_id": device.id}
            )

    async def _restore(self, device: Device, health: DeviceHealth, reason: str) -> None:
        await self._registry.set_state(device.id, DeviceState.ONLINE, note=reason)
        await self._emit(
            {"type": "device.restored", "device_id": device.id, "reason": reason}
        )
        log.info(
            "health.restored",
            device_id=device.id,
            reason=reason,
            lifetime_recoveries=device.recoveries,
        )

    async def _retire(self, device: Device, health: DeviceHealth) -> None:
        await self._registry.set_state(
            device.id,
            DeviceState.RETIRED,
            note=(
                f"{self._policy.retire_after_recoveries} recoveries in "
                f"{int(self._policy.recovery_window_s)}s -- needs a human"
            ),
        )
        await self._sessions.invalidate(device.id)
        await self._emit({"type": "device.retired", "device_id": device.id})
        log.error(
            "health.retired",
            device_id=device.id,
            recoveries_in_window=health.recent_recoveries(
                self._policy.recovery_window_s
            ),
        )

    async def _emit(self, event: dict[str, Any]) -> None:
        event.setdefault("ts", time.time())
        if self._sink is not None:
            await self._sink(event)
