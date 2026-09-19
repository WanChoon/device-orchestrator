"""Tests for the behaviour that is hard to demonstrate on real hardware.

Anything that needs a phone to misbehave is tested here with an injected probe
and a fake session, because "unplug the cable at the right moment" is not a
repeatable test. Run with:

    python -m unittest discover -s tests -v
"""

from __future__ import annotations

import asyncio
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.orchestrator import Orchestrator  # noqa: E402
from api.ws import EventHub  # noqa: E402
from core.device import (  # noqa: E402
    Device,
    DeviceRegistry,
    DeviceState,
    StaticDeviceSource,
    _parse_devices,
    _transport_for,
)
from core.health import HealthPolicy  # noqa: E402
from core.scheduler import SchedulerPolicy  # noqa: E402
from core.session import SessionManager  # noqa: E402
from core.task import DeviceSelector, FatalError, TaskSpec, TaskState  # noqa: E402
from obs.log import configure  # noqa: E402
from targets.android import AndroidTarget, FakeAppiumSessionFactory  # noqa: E402
from targets.web import FakeBrowserSessionFactory, WebTarget, browser_slots  # noqa: E402

configure("CRITICAL")

ANDROID_STEPS = [
    {"op": "tap", "id": "start"},
    {"op": "read", "id": "status", "into": "status"},
]


def phones(*ids: str) -> list[Device]:
    return [
        Device(id=i, transport="usb", model="test", tags=("android",)) for i in ids
    ]


async def build(
    devices: list[Device],
    *,
    should_fail=None,
    probe=None,
    workers: int = 2,
    browsers: int = 1,
) -> Orchestrator:
    sessions = SessionManager()
    return Orchestrator(
        source=StaticDeviceSource(devices + browser_slots(browsers)),
        targets={
            "android": AndroidTarget(
                sessions, FakeAppiumSessionFactory(should_fail=should_fail)
            ),
            "web": WebTarget(sessions, FakeBrowserSessionFactory()),
        },
        sessions=sessions,
        scheduler_policy=SchedulerPolicy(workers=workers, retry_base_s=0.05),
        health_policy=HealthPolicy(
            interval_s=0.5, failures_to_quarantine=1, recovery_attempts=1
        ),
        hub=EventHub(),
        probe=probe,
    )


class ParsingTests(unittest.TestCase):
    def test_transport_inference(self) -> None:
        self.assertEqual(_transport_for("R9WABC123"), "usb")
        self.assertEqual(_transport_for("192.168.1.20:5555"), "tcp")
        # A loopback target is a tunnelled phone, not a phone on the LAN -- the
        # distinction drives which recovery path applies.
        self.assertEqual(_transport_for("127.0.0.1:28091"), "tunnel")

    def test_adb_devices_parsing(self) -> None:
        raw = (
            "List of devices attached\n"
            "R9WABC123      device product:x model:Pixel_7 device:y\n"
            "192.168.1.20:5555   offline\n"
            "BADSERIAL      unauthorized\n"
            "\n"
        )
        devices = _parse_devices(raw)
        self.assertEqual(len(devices), 3)
        self.assertEqual(devices[0].state, DeviceState.ONLINE)
        self.assertEqual(devices[0].model, "Pixel_7")
        self.assertEqual(devices[1].state, DeviceState.OFFLINE)
        self.assertEqual(devices[2].state, DeviceState.UNAUTHORIZED)


class SelectorTests(unittest.TestCase):
    def test_tags_must_all_match(self) -> None:
        device = Device(id="d1", transport="usb", tags=("android", "sim-a"))
        self.assertTrue(device.matches(DeviceSelector(tags=("android",))))
        self.assertFalse(device.matches(DeviceSelector(tags=("android", "sim-b"))))

    def test_transport_filter(self) -> None:
        device = Device(id="d1", transport="tunnel")
        self.assertTrue(device.matches(DeviceSelector(transports=("tunnel", "usb"))))
        self.assertFalse(device.matches(DeviceSelector(transports=("usb",))))


class RegistryTests(unittest.IsolatedAsyncioTestCase):
    async def test_lease_is_exclusive(self) -> None:
        registry = DeviceRegistry(StaticDeviceSource(phones("d1")))
        await registry.refresh()

        first = await registry.acquire(DeviceSelector(), timeout=1)
        self.assertIsNotNone(first)
        second = await registry.acquire(DeviceSelector(), timeout=0.2)
        self.assertIsNone(second, "a leased device must not be handed out twice")

        assert first is not None
        await registry.release(first)
        third = await registry.acquire(DeviceSelector(), timeout=1)
        self.assertIsNotNone(third, "release must make the device available again")

    async def test_quarantined_device_is_not_assignable(self) -> None:
        registry = DeviceRegistry(StaticDeviceSource(phones("d1")))
        await registry.refresh()
        await registry.set_state("d1", DeviceState.QUARANTINED, note="test")
        self.assertIsNone(await registry.acquire(DeviceSelector(), timeout=0.2))

    async def test_refresh_does_not_resurrect_a_quarantined_device(self) -> None:
        # adb reporting "device" must not overrule a health decision; otherwise
        # the next poll un-quarantines a phone that is still broken.
        registry = DeviceRegistry(StaticDeviceSource(phones("d1")))
        await registry.refresh()
        await registry.set_state("d1", DeviceState.QUARANTINED, note="test")
        await registry.refresh()
        device = registry.get("d1")
        assert device is not None
        self.assertEqual(device.state, DeviceState.QUARANTINED)


class SchedulerTests(unittest.IsolatedAsyncioTestCase):
    async def test_happy_path(self) -> None:
        orchestrator = await build(phones("d1", "d2"))
        await orchestrator.start()
        try:
            for index in range(4):
                orchestrator.scheduler.submit(
                    TaskSpec(kind="android", steps=tuple(ANDROID_STEPS), id=f"t{index}")
                )
            self.assertTrue(await orchestrator.scheduler.drain(timeout=20))
            for record in orchestrator.scheduler.records():
                self.assertEqual(record.state, TaskState.SUCCEEDED, record.error)
        finally:
            await orchestrator.stop()

    async def test_dead_device_task_lands_on_a_healthy_one(self) -> None:
        """The headline behaviour: one device dies, the work still finishes."""
        dead = {"d2"}
        orchestrator = await build(
            phones("d1", "d2"),
            should_fail=lambda device_id: device_id in dead,
            probe=lambda device: asyncio.sleep(0, result=device.id not in dead),
            workers=1,  # serialised so the outcome is deterministic
        )
        await orchestrator.start()
        try:
            orchestrator.scheduler.submit(
                TaskSpec(
                    kind="android",
                    steps=tuple(ANDROID_STEPS),
                    id="survivor",
                    max_attempts=4,
                )
            )
            self.assertTrue(await orchestrator.scheduler.drain(timeout=30))
            record = orchestrator.scheduler.get("survivor")
            assert record is not None
            self.assertEqual(record.state, TaskState.SUCCEEDED, record.error)
            self.assertEqual(record.attempts[-1].device_id, "d1")

            broken = orchestrator.registry.get("d2")
            assert broken is not None
            self.assertIn(
                broken.state, (DeviceState.QUARANTINED, DeviceState.RETIRED)
            )
        finally:
            await orchestrator.stop()

    async def test_unschedulable_selector_is_abandoned_not_hung(self) -> None:
        orchestrator = await build(phones("d1"))
        await orchestrator.start()
        try:
            orchestrator.scheduler.submit(
                TaskSpec(
                    kind="android",
                    steps=tuple(ANDROID_STEPS),
                    id="nope",
                    selector=DeviceSelector(tags=("ios",)),
                )
            )
            self.assertTrue(await orchestrator.scheduler.drain(timeout=10))
            record = orchestrator.scheduler.get("nope")
            assert record is not None
            self.assertEqual(record.state, TaskState.ABANDONED)
            self.assertEqual(record.attempt_count, 0, "must not burn a device slot")
        finally:
            await orchestrator.stop()

    async def test_unknown_step_is_rejected_without_retrying(self) -> None:
        # A bug in the caller must not be retried across the whole fleet.
        orchestrator = await build(phones("d1", "d2"))
        await orchestrator.start()
        try:
            orchestrator.scheduler.submit(
                TaskSpec(
                    kind="android",
                    steps=({"op": "teleport"},),
                    id="bad-op",
                    max_attempts=5,
                )
            )
            self.assertTrue(await orchestrator.scheduler.drain(timeout=10))
            record = orchestrator.scheduler.get("bad-op")
            assert record is not None
            self.assertEqual(record.state, TaskState.REJECTED)
            self.assertEqual(record.attempt_count, 1)
        finally:
            await orchestrator.stop()

    async def test_unknown_kind_is_rejected_at_submit(self) -> None:
        orchestrator = await build(phones("d1"))
        record = orchestrator.scheduler.submit(
            TaskSpec(kind="ios", steps=(), id="wrong-kind")
        )
        self.assertEqual(record.state, TaskState.REJECTED)

    async def test_drain_waits_for_a_retry_still_in_backoff(self) -> None:
        # Regression: drain() used to report idle while a retry was sleeping,
        # so a caller walked away from work that had not finished.
        flaky = {"d1"}

        def should_fail(device_id: str) -> bool:
            if device_id in flaky:
                flaky.discard(device_id)  # fails once, then recovers
                return True
            return False

        orchestrator = await build(phones("d1"), should_fail=should_fail, workers=1)
        await orchestrator.start()
        try:
            orchestrator.scheduler.submit(
                TaskSpec(
                    kind="android", steps=tuple(ANDROID_STEPS), id="retried",
                    max_attempts=3,
                )
            )
            self.assertTrue(await orchestrator.scheduler.drain(timeout=30))
            record = orchestrator.scheduler.get("retried")
            assert record is not None
            self.assertIn(
                record.state,
                (TaskState.SUCCEEDED, TaskState.FAILED),
                "drain returned while the task was still pending",
            )
        finally:
            await orchestrator.stop()


class TargetContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_both_targets_satisfy_one_contract(self) -> None:
        """Android and web go through the same scheduler with no special case."""
        orchestrator = await build(phones("d1"), browsers=1)
        await orchestrator.start()
        try:
            orchestrator.scheduler.submit(
                TaskSpec(
                    kind="android",
                    steps=tuple(ANDROID_STEPS),
                    id="a1",
                    selector=DeviceSelector(tags=("android",)),
                )
            )
            orchestrator.scheduler.submit(
                TaskSpec(
                    kind="web",
                    steps=({"op": "goto", "url": "https://example.com"},),
                    id="w1",
                    selector=DeviceSelector(tags=("web",)),
                )
            )
            self.assertTrue(await orchestrator.scheduler.drain(timeout=20))
            for task_id in ("a1", "w1"):
                record = orchestrator.scheduler.get(task_id)
                assert record is not None
                self.assertEqual(record.state, TaskState.SUCCEEDED, record.error)
                assert record.result is not None
                self.assertIn("session_generation", record.result)
        finally:
            await orchestrator.stop()


class RecoveryTests(unittest.IsolatedAsyncioTestCase):
    """A background recovery is work whose premise can expire while it runs."""

    async def _monitor(self, probe_ok):
        from core.device import Adb, DeviceRegistry, StaticDeviceSource
        from core.health import HealthMonitor, HealthPolicy

        registry = DeviceRegistry(StaticDeviceSource(phones("phone-1")))
        await registry.refresh()

        class DeadAdb(Adb):
            def available(self) -> bool:
                return True

            async def reconnect(self, serial, timeout=10.0) -> bool:
                return False

        events = []

        async def sink(event):
            events.append(event)

        monitor = HealthMonitor(
            registry,
            DeadAdb(),
            SessionManager(),
            policy=HealthPolicy(
                interval_s=60.0,              # no sweeps; drive it by hand
                recovery_attempts=1,
                recovery_backoff_base_s=4.0,
                recovery_backoff_cap_s=32.0,
            ),
            sink=sink,
            probe=probe_ok,
        )
        return registry, monitor, events

    async def test_a_recovery_that_finishes_after_a_restore_says_nothing(self) -> None:
        async def never(device):
            return False

        registry, monitor, events = await self._monitor(never)
        device = registry.all()[0]
        await monitor.quarantine("phone-1", reason="test")
        events.clear()

        # Something else brings it back while the episode is still running --
        # the sweep probe succeeded, or an operator fixed the cable.
        await registry.set_state("phone-1", DeviceState.ONLINE, note="probe recovered")
        await monitor._attempt_recovery(device, monitor.health_for("phone-1"))

        # Its verdict is about a world that no longer exists. Publishing
        # `recovery_failed` for a device that is currently ONLINE is a lie that
        # looks like telemetry.
        kinds = [e["type"] for e in events]
        self.assertNotIn("device.recovery_failed", kinds, kinds)

    async def test_repeated_failures_back_off_instead_of_flooding(self) -> None:
        async def never(device):
            return False

        registry, monitor, events = await self._monitor(never)
        device = registry.all()[0]
        health = monitor.health_for("phone-1")
        await monitor.quarantine("phone-1", reason="test")

        delays = []
        for _ in range(4):
            health.next_recovery_at = 0.0          # pretend the wait elapsed
            await monitor._attempt_recovery(device, health)
            failed = [e for e in events if e["type"] == "device.recovery_failed"]
            delays.append(failed[-1]["next_try_in_s"])

        # A phone that is simply gone fails every episode forever. Without
        # growth, one dead device emits an event every few seconds and pushes
        # everything anyone needed out of a bounded feed.
        self.assertEqual(delays, sorted(delays))
        self.assertGreater(delays[-1], delays[0])
        self.assertLessEqual(delays[-1], 32.0)

        # Each line has to say something different, or it reads as spam.
        # (quarantine() spawns an episode of its own, so count what arrived
        # rather than assuming only the hand-driven ones are here.)
        failed_events = [e for e in events if e["type"] == "device.recovery_failed"]
        reasons = {e["reason"] for e in failed_events}
        self.assertEqual(len(reasons), len(failed_events))

    async def test_coming_back_clears_the_backoff(self) -> None:
        async def never(device):
            return False

        registry, monitor, events = await self._monitor(never)
        device = registry.all()[0]
        health = monitor.health_for("phone-1")

        await monitor.quarantine("phone-1", reason="test")
        health.next_recovery_at = 0.0
        await monitor._attempt_recovery(device, health)
        self.assertGreater(health.failed_recoveries, 0)

        await monitor._restore(device, health, reason="probe recovered")

        # The next outage is a fresh problem and deserves to be chased
        # immediately, not throttled by what the last one earned.
        self.assertEqual(health.failed_recoveries, 0)
        self.assertEqual(health.next_recovery_at, 0.0)


class SpecTests(unittest.TestCase):
    def test_missing_kind_is_fatal(self) -> None:
        with self.assertRaises(FatalError):
            TaskSpec.from_dict({"steps": []})

    def test_round_trip(self) -> None:
        spec = TaskSpec.from_dict(
            {
                "kind": "android",
                "steps": ANDROID_STEPS,
                "selector": {"tags": ["android"], "transports": ["usb"]},
                "timeout_s": 30,
            }
        )
        again = TaskSpec.from_dict(spec.to_dict())
        self.assertEqual(again.to_dict(), spec.to_dict())


if __name__ == "__main__":
    unittest.main()
