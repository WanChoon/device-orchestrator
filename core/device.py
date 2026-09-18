"""Device inventory, the adb subprocess wrapper, and exclusive leases.

Two ideas carry this module.

1. `Adb` never blocks the event loop and never runs without a deadline. Every
   adb invocation goes through `Adb.run`, which uses
   `asyncio.create_subprocess_exec` and kills the process group on timeout. A
   phone that has stopped answering makes `adb` hang forever rather than fail,
   so a missing timeout is not a small bug -- it is how one bad device stalls
   the whole fleet.

2. A device is held by a `Lease`, not by convention. Two tasks on one phone at
   once produce failures that look like flaky selectors and cost days to
   diagnose, so exclusivity is enforced by the registry rather than trusted to
   callers.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import shutil
import signal
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional, Protocol, Sequence

from core.task import DeviceSelector
from obs.log import get_logger

log = get_logger("core.device")


class DeviceState(str, Enum):
    ONLINE = "online"
    OFFLINE = "offline"
    UNAUTHORIZED = "unauthorized"
    QUARANTINED = "quarantined"   # health monitor pulled it out of the pool
    RETIRED = "retired"           # too many recoveries; needs a human


# adb reports these in `adb devices`; anything else is treated as offline.
_ADB_STATE_MAP = {
    "device": DeviceState.ONLINE,
    "offline": DeviceState.OFFLINE,
    "unauthorized": DeviceState.UNAUTHORIZED,
}

ASSIGNABLE_STATES = frozenset({DeviceState.ONLINE})


@dataclass
class Device:
    id: str                       # adb serial, or host:port for tcp/tunnel
    transport: str = "usb"        # usb | tcp | tunnel | virtual
    state: DeviceState = DeviceState.ONLINE
    model: str = ""
    tags: tuple[str, ...] = ()
    last_seen: float = field(default_factory=time.time)
    recoveries: int = 0           # how many times health had to intervene
    note: str = ""

    def matches(self, selector: DeviceSelector) -> bool:
        if selector.device_id and selector.device_id != self.id:
            return False
        if selector.transports and self.transport not in selector.transports:
            return False
        if selector.tags and not set(selector.tags).issubset(set(self.tags)):
            return False
        return True

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "transport": self.transport,
            "state": self.state.value,
            "model": self.model,
            "tags": list(self.tags),
            "last_seen": self.last_seen,
            "recoveries": self.recoveries,
            "note": self.note,
        }


@dataclass
class AdbResult:
    returncode: int
    stdout: str
    stderr: str
    timed_out: bool = False

    @property
    def ok(self) -> bool:
        return self.returncode == 0 and not self.timed_out


class AdbNotAvailable(RuntimeError):
    pass


class Adb:
    """Async wrapper over the adb CLI. Every call is bounded."""

    def __init__(self, binary: str = "adb", default_timeout: float = 10.0) -> None:
        self.binary = binary
        self.default_timeout = default_timeout

    def available(self) -> bool:
        return shutil.which(self.binary) is not None

    async def run(
        self,
        *args: str,
        serial: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> AdbResult:
        if not self.available():
            raise AdbNotAvailable(f"{self.binary} is not on PATH")

        argv: list[str] = [self.binary]
        if serial:
            argv += ["-s", serial]
        argv += list(args)
        budget = timeout if timeout is not None else self.default_timeout

        started = time.monotonic()
        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            # New process group so a timeout kills adb and anything it spawned,
            # not just the parent. On Windows the equivalent flag is set below.
            **_spawn_kwargs(),
        )
        try:
            out, err = await asyncio.wait_for(proc.communicate(), timeout=budget)
        except asyncio.TimeoutError:
            _kill_tree(proc)
            with contextlib.suppress(Exception):
                await asyncio.wait_for(proc.wait(), timeout=2.0)
            log.warning(
                "adb.timeout",
                argv=" ".join(argv),
                serial=serial,
                timeout_s=budget,
            )
            return AdbResult(returncode=-1, stdout="", stderr="timeout", timed_out=True)

        result = AdbResult(
            returncode=proc.returncode if proc.returncode is not None else -1,
            stdout=out.decode(errors="replace"),
            stderr=err.decode(errors="replace"),
        )
        log.debug(
            "adb.call",
            argv=" ".join(argv),
            serial=serial,
            rc=result.returncode,
            elapsed_ms=round((time.monotonic() - started) * 1000),
        )
        return result

    async def devices(self) -> list[Device]:
        result = await self.run("devices", "-l", timeout=self.default_timeout)
        if not result.ok:
            return []
        return _parse_devices(result.stdout)

    async def ping(self, serial: str, timeout: float = 3.0) -> bool:
        """Liveness probe. A tunnelled device can be LISTENing and still be a
        black hole -- the socket accepts, adb never answers. Only a real shell
        round trip inside a short deadline proves the path end to end."""
        try:
            result = await self.run("shell", "true", serial=serial, timeout=timeout)
        except AdbNotAvailable:
            return False
        return result.ok

    async def reconnect(self, serial: str, timeout: float = 10.0) -> bool:
        if ":" in serial:  # tcp / tunnel target
            await self.run("disconnect", serial, timeout=timeout)
            result = await self.run("connect", serial, timeout=timeout)
            return result.ok and "connected" in result.stdout.lower()
        result = await self.run("reconnect", serial=serial, timeout=timeout)
        return result.ok


def _spawn_kwargs() -> dict[str, Any]:
    if os.name == "nt":
        creationflags = getattr(__import__("subprocess"), "CREATE_NEW_PROCESS_GROUP", 0)
        return {"creationflags": creationflags}
    return {"start_new_session": True}


def _kill_tree(proc: asyncio.subprocess.Process) -> None:
    with contextlib.suppress(ProcessLookupError, OSError):
        if os.name == "nt":
            proc.kill()
        else:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)


def _parse_devices(raw: str) -> list[Device]:
    devices: list[Device] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("List of devices"):
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        serial, raw_state = parts[0], parts[1]
        props = dict(
            piece.split(":", 1) for piece in parts[2:] if ":" in piece
        )
        devices.append(
            Device(
                id=serial,
                transport=_transport_for(serial),
                state=_ADB_STATE_MAP.get(raw_state, DeviceState.OFFLINE),
                model=props.get("model", ""),
            )
        )
    return devices


def _transport_for(serial: str) -> str:
    if serial.startswith("127.0.0.1:") or serial.startswith("localhost:"):
        # A loopback target is an SSH-forwarded phone, not a phone on the LAN.
        return "tunnel"
    if ":" in serial:
        return "tcp"
    return "usb"


class DeviceSource(Protocol):
    """Where the registry learns which devices exist."""

    async def poll(self) -> Sequence[Device]:
        ...


class AdbDeviceSource:
    def __init__(self, adb: Adb, tags: Optional[dict[str, Sequence[str]]] = None) -> None:
        self._adb = adb
        self._tags = {k: tuple(v) for k, v in (tags or {}).items()}

    async def poll(self) -> Sequence[Device]:
        devices = await self._adb.devices()
        for device in devices:
            device.tags = self._tags.get(device.id, ())
        return devices


class StaticDeviceSource:
    """A fixed inventory. Used by the demo, and by tests that must not shell out."""

    def __init__(self, devices: Sequence[Device]) -> None:
        self._devices = list(devices)

    async def poll(self) -> Sequence[Device]:
        return list(self._devices)


@dataclass
class Lease:
    """Exclusive, time-stamped hold on one device."""

    device: Device
    token: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    acquired_at: float = field(default_factory=time.monotonic)
    released: bool = False

    @property
    def device_id(self) -> str:
        return self.device.id

    @property
    def held_s(self) -> float:
        return time.monotonic() - self.acquired_at


class DeviceRegistry:
    """The only place that knows which devices exist and who holds them."""

    def __init__(self, source: DeviceSource) -> None:
        self._source = source
        self._devices: dict[str, Device] = {}
        self._leases: dict[str, Lease] = {}
        self._cond = asyncio.Condition()

    async def refresh(self) -> None:
        """Reconcile the inventory with the source.

        A device that disappears is kept in the table as OFFLINE rather than
        deleted, so its recovery count and its history survive a flap. Deleting
        it would reset the circuit breaker every time the cable is nudged.
        """
        discovered = {d.id: d for d in await self._source.poll()}
        async with self._cond:
            for device_id, found in discovered.items():
                existing = self._devices.get(device_id)
                if existing is None:
                    self._devices[device_id] = found
                    log.info(
                        "device.discovered",
                        device_id=device_id,
                        transport=found.transport,
                        state=found.state.value,
                    )
                    continue
                existing.last_seen = time.time()
                existing.model = found.model or existing.model
                existing.tags = found.tags or existing.tags
                # Quarantine and retirement are decisions made by the health
                # monitor. adb saying "device" does not overrule them.
                if existing.state not in (DeviceState.QUARANTINED, DeviceState.RETIRED):
                    if existing.state != found.state:
                        log.info(
                            "device.state_changed",
                            device_id=device_id,
                            was=existing.state.value,
                            now=found.state.value,
                        )
                    existing.state = found.state

            for device_id, device in self._devices.items():
                if device_id not in discovered and device.state == DeviceState.ONLINE:
                    log.warning("device.vanished", device_id=device_id)
                    device.state = DeviceState.OFFLINE

            self._cond.notify_all()

    def get(self, device_id: str) -> Optional[Device]:
        return self._devices.get(device_id)

    def all(self) -> list[Device]:
        return list(self._devices.values())

    def snapshot(self) -> list[dict[str, Any]]:
        held = set(self._leases)
        return [
            {**d.to_dict(), "leased": d.id in held}
            for d in sorted(self._devices.values(), key=lambda d: d.id)
        ]

    def _find_free(self, selector: DeviceSelector) -> Optional[Device]:
        for device in self._devices.values():
            if device.id in self._leases:
                continue
            if device.state not in ASSIGNABLE_STATES:
                continue
            if device.matches(selector):
                return device
        return None

    def could_ever_match(self, selector: DeviceSelector) -> bool:
        """False means no device in the inventory can satisfy this selector even
        if every lease is released -- the task is unschedulable, not merely
        waiting, and the scheduler should abandon it instead of blocking."""
        return any(
            d.matches(selector)
            for d in self._devices.values()
            if d.state != DeviceState.RETIRED
        )

    async def acquire(
        self, selector: DeviceSelector, timeout: Optional[float] = None
    ) -> Optional[Lease]:
        """Wait for a matching free device. None on timeout.

        Uses a Condition rather than a poll loop so a freed device is picked up
        immediately and an idle fleet costs no CPU.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        async with self._cond:
            while True:
                device = self._find_free(selector)
                if device is not None:
                    lease = Lease(device=device)
                    self._leases[device.id] = lease
                    log.info(
                        "device.leased",
                        device_id=device.id,
                        lease=lease.token,
                        transport=device.transport,
                    )
                    return lease

                if deadline is None:
                    await self._cond.wait()
                    continue
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                try:
                    await asyncio.wait_for(self._cond.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    return None

    async def release(self, lease: Lease) -> None:
        if lease.released:
            return
        lease.released = True
        async with self._cond:
            current = self._leases.get(lease.device_id)
            if current is not None and current.token == lease.token:
                del self._leases[lease.device_id]
            log.info(
                "device.released",
                device_id=lease.device_id,
                lease=lease.token,
                held_s=round(lease.held_s, 2),
            )
            self._cond.notify_all()

    async def set_state(self, device_id: str, state: DeviceState, note: str = "") -> None:
        async with self._cond:
            device = self._devices.get(device_id)
            if device is None:
                return
            if device.state == state:
                return
            log.info(
                "device.state_forced",
                device_id=device_id,
                was=device.state.value,
                now=state.value,
                note=note,
            )
            device.state = state
            device.note = note
            self._cond.notify_all()

    def is_leased(self, device_id: str) -> bool:
        return device_id in self._leases
