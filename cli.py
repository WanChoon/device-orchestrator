#!/usr/bin/env python3
"""device-orchestrator CLI.

    python cli.py demo                  end-to-end run, no adb/appium/browser needed
    python cli.py devices               what adb currently sees
    python cli.py run --file task.json  submit one task and wait for it
    python cli.py serve                 HTTP + WebSocket API

`demo` exists because the interesting behaviour of this system is what happens
when a device stops answering, and that is exactly the behaviour you cannot show
on a laptop with no phones attached. It injects a device that goes dark
mid-flight and lets the real scheduler, health monitor and session manager
handle it.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any, Optional

from api.server import Orchestrator, create_app
from api.ws import EventHub
from core.device import Adb, AdbDeviceSource, Device, StaticDeviceSource
from core.health import HealthPolicy
from core.scheduler import SchedulerPolicy
from core.session import SessionManager
from core.task import FatalError, TaskSpec, TaskState
from obs.log import configure, get_logger
from targets.android import AndroidTarget, AppiumSessionFactory, FakeAppiumSessionFactory
from targets.web import (
    FakeBrowserSessionFactory,
    PLAYWRIGHT_AVAILABLE,
    PlaywrightSessionFactory,
    WebTarget,
    browser_slots,
)

log = get_logger("cli")


def build_orchestrator(
    *,
    fake: bool,
    appium_url: str,
    browser_count: int,
    workers: int,
    devices: Optional[list[Device]] = None,
    probe: Optional[Any] = None,
    health_policy: Optional[HealthPolicy] = None,
    fake_should_fail: Optional[Any] = None,
    fake_step_delay_s: float = 0.02,
) -> Orchestrator:
    adb = Adb()
    sessions = SessionManager()

    android_factory = (
        FakeAppiumSessionFactory(
            should_fail=fake_should_fail, step_delay_s=fake_step_delay_s
        )
        if fake
        else AppiumSessionFactory(appium_url)
    )
    web_factory = (
        FakeBrowserSessionFactory()
        if fake or not PLAYWRIGHT_AVAILABLE
        else PlaywrightSessionFactory()
    )

    slots = browser_slots(browser_count)
    if devices is not None:
        source: Any = StaticDeviceSource(list(devices) + slots)
    elif fake:
        source = StaticDeviceSource(slots)
    else:
        # Browser slots are static; phones come from adb. Both land in the same
        # registry, which is the whole point of modelling a slot as a device.
        source = _CompositeSource(AdbDeviceSource(adb), slots)

    return Orchestrator(
        source=source,
        targets={
            "android": AndroidTarget(sessions, android_factory),
            "web": WebTarget(sessions, web_factory),
        },
        adb=adb,
        sessions=sessions,
        scheduler_policy=SchedulerPolicy(workers=workers),
        health_policy=health_policy,
        hub=EventHub(),
        probe=probe,
    )


class _CompositeSource:
    """Merges several device sources into one inventory."""

    def __init__(self, *sources: Any) -> None:
        self._sources = sources

    async def poll(self) -> list[Device]:
        merged: list[Device] = []
        for source in self._sources:
            if isinstance(source, list):
                merged.extend(source)
            else:
                merged.extend(await source.poll())
        return merged


# ---------------------------------------------------------------- commands


async def cmd_devices(args: argparse.Namespace) -> int:
    adb = Adb()
    if not adb.available():
        print("adb is not on PATH", file=sys.stderr)
        return 2
    devices = await adb.devices()
    for device in devices:
        alive = await adb.ping(device.id, timeout=3.0)
        print(
            f"{device.id:<28} {device.transport:<8} {device.state.value:<12} "
            f"responsive={'yes' if alive else 'NO'}  {device.model}"
        )
    if not devices:
        print("(no devices)")
    return 0


async def cmd_run(args: argparse.Namespace) -> int:
    payload = json.loads(Path(args.file).read_text(encoding="utf-8"))
    specs = payload if isinstance(payload, list) else [payload]

    orchestrator = build_orchestrator(
        fake=args.fake,
        appium_url=args.appium_url,
        browser_count=args.browsers,
        workers=args.workers,
    )
    await orchestrator.start()
    try:
        records = []
        for raw in specs:
            try:
                records.append(orchestrator.scheduler.submit(TaskSpec.from_dict(raw)))
            except FatalError as exc:
                print(f"rejected: {exc}", file=sys.stderr)
                return 2

        await orchestrator.scheduler.drain(timeout=args.wait)
        failed = 0
        for record in records:
            fresh = orchestrator.scheduler.get(record.spec.id)
            assert fresh is not None
            print(json.dumps(fresh.to_dict(), indent=2))
            if fresh.state is not TaskState.SUCCEEDED:
                failed += 1
        return 1 if failed else 0
    finally:
        await orchestrator.stop()


async def cmd_serve(args: argparse.Namespace) -> int:
    import uvicorn

    orchestrator = build_orchestrator(
        fake=args.fake,
        appium_url=args.appium_url,
        browser_count=args.browsers,
        workers=args.workers,
    )
    app = create_app(orchestrator)
    config = uvicorn.Config(
        app, host=args.host, port=args.port, log_config=None, access_log=False
    )
    await uvicorn.Server(config).serve()
    return 0


async def cmd_demo(args: argparse.Namespace) -> int:
    """A scripted fleet with one device that dies, so the failure path is visible."""
    phones = [
        Device(id="demo-phone-1", transport="usb", model="Pixel", tags=("android",)),
        Device(id="demo-phone-2", transport="tunnel", model="Galaxy", tags=("android",)),
        Device(id="demo-phone-3", transport="tcp", model="Redmi", tags=("android",)),
    ]
    doomed = "demo-phone-3"
    loop = asyncio.get_running_loop()
    started = loop.time()

    def is_dark(device_id: str) -> bool:
        # demo-phone-3 stops answering 2 seconds in and never comes back. Its
        # in-flight task fails mid-step, the device is quarantined, recovery
        # fails, and the queue drains on the two survivors.
        return device_id == doomed and (loop.time() - started) >= args.die_after

    async def scripted_probe(device: Device) -> bool:
        return not is_dark(device.id)

    orchestrator = build_orchestrator(
        fake=True,
        appium_url=args.appium_url,
        browser_count=2,
        workers=args.workers,
        devices=phones,
        probe=scripted_probe,
        fake_should_fail=is_dark,
        fake_step_delay_s=0.25,
        health_policy=HealthPolicy(
            interval_s=2.0,
            probe_timeout_s=2.0,
            failures_to_quarantine=1,
            recovery_attempts=1,
            retire_after_recoveries=2,
            recovery_window_s=60.0,
        ),
    )

    printed: list[str] = []

    async def echo(event: dict[str, Any]) -> None:
        kind = event.get("type")
        if kind == "task.state":
            printed.append(
                f"  task {event['task_id']} -> {event['state']}"
                f" (attempt {event['attempt']}, device {event.get('device_id')})"
            )
        elif kind and kind.startswith("device."):
            printed.append(f"  ! {kind}  {event.get('device_id')} {event.get('reason', '')}")

    original = orchestrator.hub.publish

    async def tee(event: dict[str, Any]) -> None:
        await original(event)
        await echo(event)

    orchestrator.hub.publish = tee  # type: ignore[method-assign]
    orchestrator.scheduler._sink = tee  # noqa: SLF001 - demo wiring
    orchestrator.health._sink = tee  # noqa: SLF001 - demo wiring

    await orchestrator.start()
    try:
        android_steps = [
            {"op": "tap", "id": "com.example:id/start"},
            {"op": "type", "id": "com.example:id/amount", "text": "100"},
            {"op": "read", "id": "com.example:id/status", "into": "status"},
        ]
        web_steps = [
            {"op": "goto", "url": "https://example.invalid/login"},
            {"op": "fill", "selector": "#user", "text": "demo"},
            {"op": "read", "selector": "#result", "into": "result"},
        ]

        for index in range(12):
            orchestrator.scheduler.submit(
                TaskSpec.from_dict(
                    {
                        "id": f"android-{index}",
                        "kind": "android",
                        "steps": android_steps,
                        "selector": {"tags": ["android"]},
                        "timeout_s": 20,
                        "max_attempts": 3,
                    }
                )
            )
        for index in range(2):
            orchestrator.scheduler.submit(
                TaskSpec.from_dict(
                    {
                        "id": f"web-{index}",
                        "kind": "web",
                        "steps": web_steps,
                        "selector": {"tags": ["web"]},
                        "timeout_s": 20,
                    }
                )
            )
        # Proves the unschedulable path: nothing in the fleet carries this tag,
        # so it is abandoned immediately rather than blocking a worker.
        orchestrator.scheduler.submit(
            TaskSpec.from_dict(
                {
                    "id": "impossible-0",
                    "kind": "android",
                    "steps": android_steps,
                    "selector": {"tags": ["ios"]},
                }
            )
        )

        await orchestrator.scheduler.drain(timeout=args.wait)

        print("\n=== event trace ===")
        for line in printed:
            print(line)

        print("\n=== final state ===")
        for record in orchestrator.scheduler.records():
            data = record.to_dict()
            print(
                f"  {data['id']:<14} {data['state']:<10} "
                f"attempts={len(data['attempts']):<2} {data['error'] or ''}"
            )
        print("\n=== devices ===")
        for device in orchestrator.registry.snapshot():
            print(
                f"  {device['id']:<16} {device['transport']:<8} {device['state']:<12} "
                f"recoveries={device['recoveries']}  {device['note']}"
            )
        return 0
    finally:
        await orchestrator.stop()


# ---------------------------------------------------------------- entry point


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="device-orchestrator")
    parser.add_argument("--log-level", default="INFO")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def common(sub: argparse.ArgumentParser) -> None:
        sub.add_argument("--fake", action="store_true", help="no adb/appium/browser")
        sub.add_argument("--appium-url", default="http://127.0.0.1:4723")
        sub.add_argument("--browsers", type=int, default=2)
        sub.add_argument("--workers", type=int, default=4)

    devices_parser = subparsers.add_parser("devices", help="list what adb sees")
    devices_parser.set_defaults(func=cmd_devices)

    run_parser = subparsers.add_parser("run", help="submit tasks from a JSON file")
    run_parser.add_argument("--file", required=True)
    run_parser.add_argument("--wait", type=float, default=300.0)
    common(run_parser)
    run_parser.set_defaults(func=cmd_run)

    serve_parser = subparsers.add_parser("serve", help="HTTP + WebSocket API")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8080)
    common(serve_parser)
    serve_parser.set_defaults(func=cmd_serve)

    demo_parser = subparsers.add_parser("demo", help="scripted run with a dying device")
    demo_parser.add_argument("--wait", type=float, default=90.0)
    demo_parser.add_argument("--workers", type=int, default=3)
    demo_parser.add_argument(
        "--die-after", type=float, default=2.0, help="when demo-phone-3 goes dark"
    )
    demo_parser.add_argument("--appium-url", default="http://127.0.0.1:4723")
    demo_parser.set_defaults(func=cmd_demo)

    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    configure(args.log_level)
    try:
        return asyncio.run(args.func(args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
