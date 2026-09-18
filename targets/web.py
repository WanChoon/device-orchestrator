"""Web target: drives a browser through Playwright.

The reason this file exists next to `android.py` is the single most important
claim the project makes: a browser and a phone are the same kind of thing to the
scheduler. Both implement `Target`. Both get their session from
`SessionManager`. Both are leased a `Device` -- a browser slot is registered as a
device with `transport="virtual"`, so concurrency limits, health checks and
quarantine work on browsers with no special case anywhere in `core/`.

Playwright is an optional import. Without it the target still runs, backed by a
fake driver, so the orchestration can be demonstrated and tested on a machine
with no browsers installed.
"""

from __future__ import annotations

import asyncio
from typing import Any, Optional

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

log = get_logger("targets.web")

try:  # pragma: no cover - availability depends on the host
    from playwright.async_api import async_playwright  # type: ignore

    PLAYWRIGHT_AVAILABLE = True
except Exception:  # noqa: BLE001
    async_playwright = None  # type: ignore
    PLAYWRIGHT_AVAILABLE = False


class BrowserDriver:
    """Wraps one Playwright page behind the verbs the steps need."""

    def __init__(self, playwright: Any, browser: Any, context: Any, page: Any) -> None:
        self._playwright = playwright
        self._browser = browser
        self._context = context
        self.page = page

    async def goto(self, url: str, timeout_ms: int) -> None:
        await self.page.goto(url, timeout=timeout_ms, wait_until="domcontentloaded")

    async def click(self, selector: str, timeout_ms: int) -> None:
        await self.page.click(selector, timeout=timeout_ms)

    async def fill(self, selector: str, text: str, timeout_ms: int) -> None:
        await self.page.fill(selector, text, timeout=timeout_ms)

    async def text_of(self, selector: str, timeout_ms: int) -> str:
        return await self.page.inner_text(selector, timeout=timeout_ms)

    async def alive(self) -> bool:
        try:
            return not self.page.is_closed()
        except Exception:  # noqa: BLE001
            return False

    async def shutdown(self) -> None:
        for closer in (self._context.close, self._browser.close, self._playwright.stop):
            try:
                await closer()
            except Exception:  # noqa: BLE001 - best effort teardown
                pass


class PlaywrightSessionFactory:
    kind = "web"

    def __init__(self, *, headless: bool = True, browser: str = "chromium") -> None:
        self.headless = headless
        self.browser_name = browser

    async def open(self, device: Device, ctx: TaskContext) -> BrowserDriver:
        if not PLAYWRIGHT_AVAILABLE:
            raise RetryableError("playwright is not installed", blames_device=False)
        playwright = await async_playwright().start()  # type: ignore[union-attr]
        launcher = getattr(playwright, self.browser_name)
        browser = await launcher.launch(headless=self.headless)
        context = await browser.new_context()
        page = await context.new_page()
        return BrowserDriver(playwright, browser, context, page)

    async def check(self, driver: BrowserDriver) -> bool:
        return await driver.alive()

    async def close(self, driver: BrowserDriver) -> None:
        await driver.shutdown()


class FakeBrowserDriver:
    """Same surface as BrowserDriver, no browser. Used when Playwright is absent."""

    def __init__(self, slot: str) -> None:
        self.slot = slot
        self.url = "about:blank"
        self._closed = False

    async def goto(self, url: str, timeout_ms: int) -> None:
        await asyncio.sleep(0.05)
        self.url = url

    async def click(self, selector: str, timeout_ms: int) -> None:
        await asyncio.sleep(0.02)

    async def fill(self, selector: str, text: str, timeout_ms: int) -> None:
        await asyncio.sleep(0.02)

    async def text_of(self, selector: str, timeout_ms: int) -> str:
        await asyncio.sleep(0.01)
        return f"fake text for {selector}"

    async def alive(self) -> bool:
        return not self._closed

    async def shutdown(self) -> None:
        self._closed = True


class FakeBrowserSessionFactory:
    kind = "web"

    async def open(self, device: Device, ctx: TaskContext) -> FakeBrowserDriver:
        await asyncio.sleep(0.05)
        return FakeBrowserDriver(device.id)

    async def check(self, driver: FakeBrowserDriver) -> bool:
        return await driver.alive()

    async def close(self, driver: FakeBrowserDriver) -> None:
        await driver.shutdown()


class WebTarget(Target):
    kind = "web"

    def __init__(self, sessions: SessionManager, factory: Any) -> None:
        self._sessions = sessions
        self._factory = factory

    async def execute(
        self, spec: TaskSpec, lease: Lease, ctx: TaskContext
    ) -> dict[str, Any]:
        await ctx.progress("session", "acquiring browser session")
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
            remaining = ctx.remaining_s()
            if remaining <= 0:
                raise RetryableError("ran out of time mid-run", blames_device=False)
            # Every Playwright call gets the smaller of its own budget and what
            # the task has left, so a step cannot outlive the task that owns it.
            budget_ms = int(min(float(step.get("timeout_s", 15.0)), remaining) * 1000)
            op = step.get("op")
            await ctx.progress("step", f"{index}/{len(spec.steps)} {op}", step=index, op=op)
            captured.update(await self._run_step(driver, step, op, index, budget_ms))

        return {
            "steps": len(spec.steps),
            "session_generation": handle.generation,
            "captured": captured,
        }

    async def _run_step(
        self,
        driver: Any,
        step: dict[str, Any],
        op: Optional[str],
        index: int,
        budget_ms: int,
    ) -> dict[str, Any]:
        try:
            if op == "goto":
                await driver.goto(str(step["url"]), budget_ms)
                return {}
            if op == "click":
                await driver.click(str(step["selector"]), budget_ms)
                return {}
            if op == "fill":
                await driver.fill(str(step["selector"]), str(step.get("text", "")), budget_ms)
                return {}
            if op == "read":
                text = await driver.text_of(str(step["selector"]), budget_ms)
                return {str(step.get("into", f"step{index}")): text}
            if op == "wait":
                await asyncio.sleep(float(step.get("seconds", 1.0)))
                return {}
        except KeyError as exc:
            raise FatalError(f"web step {index} is missing {exc}") from exc
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - normalise Playwright errors
            # A browser failure is a retryable failure of this attempt, but it
            # does not blame the slot: relaunching the browser is cheap and the
            # slot itself is almost never the problem.
            raise RetryableError(f"web step {index} ({op}) failed: {exc}") from exc

        raise FatalError(f"unknown web op: {op!r}")


def browser_slots(count: int, prefix: str = "browser") -> list[Device]:
    """Register browser concurrency as devices so `core/` needs no web branch."""
    return [
        Device(
            id=f"{prefix}-{index}",
            transport="virtual",
            model="playwright" if PLAYWRIGHT_AVAILABLE else "fake-browser",
            tags=("web",),
        )
        for index in range(1, count + 1)
    ]
