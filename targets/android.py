"""Android target: drives a phone through an Appium (W3C WebDriver) session.

The HTTP client is stdlib `urllib` pushed onto a thread with `asyncio.to_thread`
rather than an async HTTP library. That is a deliberate trade: it keeps the
dependency list at zero for the part of the system most likely to be vendored
into someone else's environment, and Appium calls are slow enough (tens to
hundreds of ms) that the thread hop is noise. If this ever becomes the
bottleneck, `AppiumSessionFactory` is the only class that has to change.

`AndroidTarget` never opens or closes a session itself -- it asks
`SessionManager` for one. That is what makes reconnect uniform across targets
instead of a per-target loop that drifts.
"""

from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.request
from typing import Any, Callable, Optional

from core.device import Device, Lease
from core.session import SessionHandle, SessionManager
from core.task import (
    FatalError,
    RetryableError,
    Target,
    TaskContext,
    TaskSpec,
)
from obs.log import get_logger

log = get_logger("targets.android")

DEFAULT_APPIUM_URL = "http://127.0.0.1:4723"


class AppiumDriver:
    """A minimal W3C WebDriver client -- exactly the verbs the steps below need."""

    def __init__(self, base_url: str, session_id: str, timeout: float = 30.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.session_id = session_id
        self.timeout = timeout

    async def command(
        self, method: str, path: str, body: Optional[dict[str, Any]] = None
    ) -> dict[str, Any]:
        url = f"{self.base_url}/session/{self.session_id}{path}"
        return await asyncio.to_thread(_http, method, url, body, self.timeout)

    async def find(self, using: str, value: str) -> str:
        payload = await self.command("POST", "/element", {"using": using, "value": value})
        element = payload.get("value") or {}
        # The W3C element id lives under a versioned key; take whichever is there.
        for key, ref in element.items():
            if key.startswith("element-") or key == "ELEMENT":
                return str(ref)
        raise RetryableError(f"element not found: {using}={value}")

    async def click(self, element_id: str) -> None:
        await self.command("POST", f"/element/{element_id}/click", {})

    async def send_keys(self, element_id: str, text: str) -> None:
        await self.command(
            "POST", f"/element/{element_id}/value", {"text": text, "value": list(text)}
        )

    async def text_of(self, element_id: str) -> str:
        payload = await self.command("GET", f"/element/{element_id}/text")
        return str(payload.get("value", ""))

    async def source(self) -> str:
        payload = await self.command("GET", "/source")
        return str(payload.get("value", ""))

    async def title_probe(self) -> bool:
        await self.command("GET", "/window/handles")
        return True


def _http(
    method: str, url: str, body: Optional[dict[str, Any]], timeout: float
) -> dict[str, Any]:
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(url, data=data, method=method)
    request.add_header("Content-Type", "application/json")
    request.add_header("Accept", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode(errors="replace")
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode(errors="replace")
        detail = _extract_error(raw)
        # 404 on a session route means the session is gone, not that the
        # element is missing -- that distinction drives whether we reconnect.
        if exc.code == 404 and "/session/" in url:
            raise SessionGone(detail or "session not found") from exc
        raise RetryableError(f"appium {exc.code}: {detail or raw[:200]}") from exc
    except (urllib.error.URLError, OSError, TimeoutError) as exc:
        raise SessionGone(f"appium unreachable: {exc}") from exc

    try:
        return json.loads(raw) if raw else {}
    except json.JSONDecodeError as exc:
        raise RetryableError(f"appium returned non-JSON: {raw[:200]}") from exc


def _extract_error(raw: str) -> str:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return ""
    value = payload.get("value")
    if isinstance(value, dict):
        return str(value.get("message") or value.get("error") or "")
    return str(value or "")


class SessionGone(RetryableError):
    """The remote session no longer exists. Always blames the device path."""

    def __init__(self, message: str) -> None:
        super().__init__(message, blames_device=True)


class AppiumSessionFactory:
    """Opens W3C sessions against an Appium server. Plugged into SessionManager."""

    kind = "android"

    def __init__(
        self,
        server_url: str = DEFAULT_APPIUM_URL,
        *,
        capabilities: Optional[dict[str, Any]] = None,
        new_command_timeout_s: int = 120,
    ) -> None:
        self.server_url = server_url.rstrip("/")
        self._extra_caps = capabilities or {}
        self._new_command_timeout_s = new_command_timeout_s

    def _capabilities_for(self, device: Device) -> dict[str, Any]:
        caps: dict[str, Any] = {
            "platformName": "Android",
            "appium:automationName": "UiAutomator2",
            "appium:udid": device.id,
            # noReset/skipDeviceInitialization keep a warm device warm; a full
            # re-init per task is the single biggest avoidable cost in a fleet.
            "appium:noReset": True,
            "appium:skipDeviceInitialization": True,
            "appium:newCommandTimeout": self._new_command_timeout_s,
        }
        caps.update(self._extra_caps)
        return caps

    async def open(self, device: Device, ctx: TaskContext) -> AppiumDriver:
        body = {
            "capabilities": {
                "alwaysMatch": self._capabilities_for(device),
                "firstMatch": [{}],
            }
        }
        timeout = max(5.0, min(90.0, ctx.remaining_s()))
        payload = await asyncio.to_thread(
            _http, "POST", f"{self.server_url}/session", body, timeout
        )
        value = payload.get("value") or {}
        session_id = value.get("sessionId") or payload.get("sessionId")
        if not session_id:
            raise RetryableError(f"appium did not return a sessionId: {payload}")
        return AppiumDriver(self.server_url, str(session_id))

    async def check(self, driver: AppiumDriver) -> bool:
        try:
            return await driver.title_probe()
        except Exception:  # noqa: BLE001 - any failure means "not alive"
            return False

    async def close(self, driver: AppiumDriver) -> None:
        try:
            await asyncio.to_thread(
                _http,
                "DELETE",
                f"{driver.base_url}/session/{driver.session_id}",
                None,
                10.0,
            )
        except Exception:  # noqa: BLE001 - deleting a dead session is expected to fail
            pass


class FakeAndroidDriver:
    """In-process stand-in so `demo` and the tests exercise the real code path.

    It implements the same surface as AppiumDriver. The scheduler, the session
    manager and AndroidTarget cannot tell the difference, which is the point:
    the thing under test is the orchestration, not the phone.
    """

    def __init__(
        self,
        device_id: str,
        *,
        fail_after: Optional[int] = None,
        should_fail: Optional[Callable[[str], bool]] = None,
        step_delay_s: float = 0.02,
    ) -> None:
        self.device_id = device_id
        self.calls = 0
        self._fail_after = fail_after
        self._should_fail = should_fail
        self._step_delay_s = step_delay_s
        self._alive = True

    def _tick(self) -> None:
        self.calls += 1
        dead_by_count = self._fail_after is not None and self.calls > self._fail_after
        dead_by_rule = self._should_fail is not None and self._should_fail(self.device_id)
        if dead_by_count or dead_by_rule:
            self._alive = False
            raise SessionGone(f"fake session on {self.device_id} stopped answering")

    async def find(self, using: str, value: str) -> str:
        self._tick()
        await asyncio.sleep(self._step_delay_s)
        return f"el-{abs(hash((using, value))) % 10000}"

    async def click(self, element_id: str) -> None:
        self._tick()
        await asyncio.sleep(self._step_delay_s)

    async def send_keys(self, element_id: str, text: str) -> None:
        self._tick()
        await asyncio.sleep(self._step_delay_s)

    async def text_of(self, element_id: str) -> str:
        self._tick()
        await asyncio.sleep(self._step_delay_s)
        return "OK"

    async def source(self) -> str:
        self._tick()
        return "<hierarchy/>"

    async def title_probe(self) -> bool:
        if self._should_fail is not None and self._should_fail(self.device_id):
            return False
        return self._alive


class FakeAppiumSessionFactory:
    kind = "android"

    def __init__(
        self,
        fail_after: Optional[int] = None,
        should_fail: Optional[Callable[[str], bool]] = None,
        step_delay_s: float = 0.02,
    ) -> None:
        self._fail_after = fail_after
        self._should_fail = should_fail
        self._step_delay_s = step_delay_s

    async def open(self, device: Device, ctx: TaskContext) -> FakeAndroidDriver:
        await asyncio.sleep(0.05)
        if self._should_fail is not None and self._should_fail(device.id):
            # A device that is already dark must fail at session-open time, the
            # way a real Appium handshake against an unreachable phone does.
            raise SessionGone(f"fake device {device.id} is unreachable")
        return FakeAndroidDriver(
            device.id,
            fail_after=self._fail_after,
            should_fail=self._should_fail,
            step_delay_s=self._step_delay_s,
        )

    async def check(self, driver: FakeAndroidDriver) -> bool:
        return await driver.title_probe()

    async def close(self, driver: FakeAndroidDriver) -> None:
        return None


class AndroidTarget(Target):
    """Interprets a task's steps against an Appium session."""

    kind = "android"

    def __init__(self, sessions: SessionManager, factory: Any) -> None:
        self._sessions = sessions
        self._factory = factory

    async def execute(
        self, spec: TaskSpec, lease: Lease, ctx: TaskContext
    ) -> dict[str, Any]:
        await ctx.progress("session", "acquiring appium session")
        handle: SessionHandle = await self._sessions.ensure(lease.device, self._factory, ctx)
        driver = handle.driver
        await ctx.progress(
            "session",
            "session ready",
            session_id=handle.id,
            generation=handle.generation,
        )

        captured: dict[str, Any] = {}
        for index, step in enumerate(spec.steps, start=1):
            if ctx.remaining_s() <= 0:
                raise RetryableError("ran out of time mid-run", blames_device=False)
            op = step.get("op")
            await ctx.progress("step", f"{index}/{len(spec.steps)} {op}", step=index, op=op)
            captured.update(await self._run_step(driver, step, op, index))

        return {
            "steps": len(spec.steps),
            "session_generation": handle.generation,
            "captured": captured,
        }

    async def _run_step(
        self, driver: Any, step: dict[str, Any], op: Optional[str], index: int
    ) -> dict[str, Any]:
        if op == "tap":
            element = await driver.find(*_locator(step))
            await driver.click(element)
            return {}
        if op == "type":
            element = await driver.find(*_locator(step))
            await driver.send_keys(element, str(step.get("text", "")))
            return {}
        if op == "read":
            element = await driver.find(*_locator(step))
            text = await driver.text_of(element)
            return {str(step.get("into", f"step{index}")): text}
        if op == "wait":
            await asyncio.sleep(float(step.get("seconds", 1.0)))
            return {}
        if op == "dump":
            return {str(step.get("into", "source")): len(await driver.source())}
        # An unknown op is a bug in the caller, not a flaky device. Retrying it
        # on another phone would waste the whole fleet failing the same way.
        raise FatalError(f"unknown android op: {op!r}")


def _locator(step: dict[str, Any]) -> tuple[str, str]:
    if "id" in step:
        return "id", str(step["id"])
    if "xpath" in step:
        return "xpath", str(step["xpath"])
    if "accessibility_id" in step:
        return "accessibility id", str(step["accessibility_id"])
    raise FatalError(f"step has no locator: {step}")
