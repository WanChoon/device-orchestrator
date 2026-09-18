"""Long-lived remote sessions, and the reconnect logic around them.

An Appium session and a browser context are both expensive to create and both
die without warning -- the phone reboots, the Appium server is restarted, a
WebSocket drops. The targets should not each grow their own reconnect loop, so
the policy lives here once and both targets borrow it.

The rule the rest of the system relies on:

    `SessionManager.ensure()` either returns a session that answered a health
    check moments ago, or raises. It never returns a stale handle.

`generation` exists so a caller can tell "the same session" from "a session that
was silently rebuilt underneath me". A target that cached page state must drop
that cache when the generation moves.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional, Protocol

from core.device import Device
from core.task import TaskContext
from obs.log import get_logger

log = get_logger("core.session")


class SessionError(RuntimeError):
    """Opening or verifying a session failed."""


@dataclass
class SessionHandle:
    device_id: str
    kind: str
    driver: Any                       # target-specific; the manager never inspects it
    generation: int = 1
    opened_at: float = field(default_factory=time.monotonic)
    id: str = field(default_factory=lambda: "sess-" + uuid.uuid4().hex[:8])

    @property
    def age_s(self) -> float:
        return time.monotonic() - self.opened_at


class SessionFactory(Protocol):
    """What a target must supply to get reconnect handling for free."""

    kind: str

    async def open(self, device: Device, ctx: TaskContext) -> Any:
        ...

    async def check(self, driver: Any) -> bool:
        """Cheap liveness probe. Must be bounded and must not raise."""
        ...

    async def close(self, driver: Any) -> None:
        ...


class SessionManager:
    """One cached session per (device, kind), rebuilt on demand."""

    def __init__(
        self,
        *,
        max_open_attempts: int = 3,
        base_backoff_s: float = 1.0,
        max_backoff_s: float = 15.0,
        max_age_s: Optional[float] = 1800.0,
    ) -> None:
        self._sessions: dict[tuple[str, str], SessionHandle] = {}
        self._locks: dict[tuple[str, str], asyncio.Lock] = {}
        self._generations: dict[tuple[str, str], int] = {}
        self._max_open_attempts = max_open_attempts
        self._base_backoff_s = base_backoff_s
        self._max_backoff_s = max_backoff_s
        self._max_age_s = max_age_s

    def _lock_for(self, key: tuple[str, str]) -> asyncio.Lock:
        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
        return lock

    async def ensure(
        self, device: Device, factory: SessionFactory, ctx: TaskContext
    ) -> SessionHandle:
        """Return a verified session, opening or reopening as needed."""
        key = (device.id, factory.kind)
        # The lock is per device+kind, not global: rebuilding one phone session
        # must not stall work on twenty other phones.
        async with self._lock_for(key):
            handle = self._sessions.get(key)

            if handle is not None and self._too_old(handle):
                log.info(
                    "session.recycled_by_age",
                    device_id=device.id,
                    kind=factory.kind,
                    age_s=round(handle.age_s, 1),
                )
                await self._discard(key, factory, handle)
                handle = None

            if handle is not None:
                if await self._check_quietly(factory, handle):
                    return handle
                log.warning(
                    "session.check_failed",
                    device_id=device.id,
                    kind=factory.kind,
                    session_id=handle.id,
                    generation=handle.generation,
                )
                await self._discard(key, factory, handle)

            return await self._open_with_backoff(key, device, factory, ctx)

    async def _open_with_backoff(
        self,
        key: tuple[str, str],
        device: Device,
        factory: SessionFactory,
        ctx: TaskContext,
    ) -> SessionHandle:
        last_error: Optional[BaseException] = None
        for attempt in range(1, self._max_open_attempts + 1):
            # Never spend longer opening a session than the task has left to
            # live; a task that is about to be cancelled should not keep a phone
            # busy with a doomed handshake.
            if ctx.remaining_s() <= 0:
                raise SessionError("task deadline passed before a session opened")
            try:
                driver = await factory.open(device, ctx)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - normalised into SessionError
                last_error = exc
                delay = min(
                    self._max_backoff_s, self._base_backoff_s * (2 ** (attempt - 1))
                )
                log.warning(
                    "session.open_failed",
                    device_id=device.id,
                    kind=factory.kind,
                    attempt=attempt,
                    of=self._max_open_attempts,
                    retry_in_s=delay,
                    reason=str(exc),
                )
                if attempt == self._max_open_attempts:
                    break
                await asyncio.sleep(min(delay, ctx.remaining_s()))
                continue

            generation = self._generations.get(key, 0) + 1
            self._generations[key] = generation
            handle = SessionHandle(
                device_id=device.id,
                kind=factory.kind,
                driver=driver,
                generation=generation,
            )
            self._sessions[key] = handle
            log.info(
                "session.opened",
                device_id=device.id,
                kind=factory.kind,
                session_id=handle.id,
                generation=generation,
                attempt=attempt,
            )
            return handle

        raise SessionError(
            f"could not open a {factory.kind} session on {device.id}: {last_error}"
        )

    async def _check_quietly(self, factory: SessionFactory, handle: SessionHandle) -> bool:
        try:
            return bool(await factory.check(handle.driver))
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - a raising check means "not alive"
            return False

    def _too_old(self, handle: SessionHandle) -> bool:
        return self._max_age_s is not None and handle.age_s > self._max_age_s

    async def _discard(
        self, key: tuple[str, str], factory: SessionFactory, handle: SessionHandle
    ) -> None:
        self._sessions.pop(key, None)
        try:
            await factory.close(handle.driver)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - closing a dead session may fail
            log.debug(
                "session.close_failed",
                device_id=handle.device_id,
                kind=handle.kind,
                reason=str(exc),
            )

    async def invalidate(self, device_id: str, kind: Optional[str] = None) -> None:
        """Drop cached sessions for a device.

        Called by the health monitor when a device is quarantined: whatever
        happens next, the handle we are holding is not going to work.
        """
        keys = [
            key
            for key in list(self._sessions)
            if key[0] == device_id and (kind is None or key[1] == kind)
        ]
        for key in keys:
            handle = self._sessions.pop(key, None)
            if handle is None:
                continue
            log.info(
                "session.invalidated",
                device_id=device_id,
                kind=key[1],
                session_id=handle.id,
            )

    def describe(self) -> list[dict[str, Any]]:
        return [
            {
                "device_id": handle.device_id,
                "kind": handle.kind,
                "session_id": handle.id,
                "generation": handle.generation,
                "age_s": round(handle.age_s, 1),
            }
            for handle in self._sessions.values()
        ]

    async def close_all(self) -> None:
        self._sessions.clear()
