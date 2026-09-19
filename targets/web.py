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
import base64
import json
import time
from typing import Any, Optional

from core.auth import AuthError, CookieJar, CredentialStore, Secret, TokenCache
from core.device import Device, Lease
from core.session import SessionHandle, SessionManager
from core.task import (
    FatalError,
    RetryableError,
    Target,
    TaskContext,
    TaskError,
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

    async def set_headers(self, headers: dict[str, str]) -> None:
        await self._context.set_extra_http_headers(headers)

    async def cookies(self) -> list[dict[str, Any]]:
        return list(await self._context.cookies())

    async def set_cookies(self, cookies: list[dict[str, Any]]) -> None:
        await self._context.add_cookies(cookies)

    async def read_storage(self, key: str) -> Optional[str]:
        # Where a single-page app keeps its JWT. Reading it back out is what
        # lets a token outlive the browser slot that obtained it.
        return await self.page.evaluate("k => window.localStorage.getItem(k)", key)

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
        self.headers: dict[str, str] = {}
        self._cookies: list[dict[str, Any]] = []
        self.storage: dict[str, str] = {}

    async def goto(self, url: str, timeout_ms: int) -> None:
        await asyncio.sleep(0.05)
        self.url = url
        if not self._cookies:
            # Standing in for a server that sets a session cookie on first
            # contact, so the jar has something real to carry between tasks.
            self._cookies = [
                {"name": "session", "value": f"fake-{self.slot}", "domain": "example.test",
                 "path": "/", "expires": time.time() + 900},
            ]
            self.storage.setdefault(
                "access_token", _fake_jwt(ttl_s=900)
            )

    async def click(self, selector: str, timeout_ms: int) -> None:
        await asyncio.sleep(0.02)

    async def fill(self, selector: str, text: str, timeout_ms: int) -> None:
        await asyncio.sleep(0.02)

    async def text_of(self, selector: str, timeout_ms: int) -> str:
        await asyncio.sleep(0.01)
        return f"fake text for {selector}"

    async def set_headers(self, headers: dict[str, str]) -> None:
        self.headers.update(headers)

    async def cookies(self) -> list[dict[str, Any]]:
        return list(self._cookies)

    async def set_cookies(self, cookies: list[dict[str, Any]]) -> None:
        self._cookies = [dict(c) for c in cookies]

    async def read_storage(self, key: str) -> Optional[str]:
        return self.storage.get(key)

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


def _fake_jwt(ttl_s: float) -> str:
    """An unsigned JWT-shaped token, so the demo exercises the real expiry path."""

    def seg(payload: dict[str, Any]) -> str:
        return base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")

    header = seg({"alg": "none", "typ": "JWT"})
    body = seg({"sub": "demo", "exp": int(time.time() + ttl_s)})
    return f"{header}.{body}.fake-signature"


class WebTarget(Target):
    """Interprets a task's steps against a browser session.

    The auth collaborators are optional and injected. A target that constructed
    its own credential store would give every deployment the same one, and the
    whole reason credentials are referenced rather than inlined is that the
    store is a deployment concern, not a task one.
    """

    kind = "web"

    def __init__(
        self,
        sessions: SessionManager,
        factory: Any,
        *,
        credentials: Optional[CredentialStore] = None,
        tokens: Optional[TokenCache] = None,
        cookies: Optional[CookieJar] = None,
    ) -> None:
        self._sessions = sessions
        self._factory = factory
        self._credentials = credentials
        self._tokens = tokens or TokenCache()
        self._cookies = cookies or CookieJar()

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
            captured.update(await self._run_step(driver, step, op, index, budget_ms, ctx))

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
        ctx: TaskContext,
    ) -> dict[str, Any]:
        try:
            if op == "auth":
                return await self._auth(driver, step, ctx)
            if op == "capture_session":
                return await self._capture_session(driver, step, ctx)
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
        except AuthError as exc:
            # A missing or rejected credential fails identically on every slot
            # in the fleet. Retrying it is a slower way to get locked out.
            raise FatalError(f"web step {index} ({op}): {exc}") from exc
        except TaskError:
            # Already classified by the step that raised it. Re-wrapping a
            # FatalError in the handler below would quietly make it retryable.
            raise
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - normalise Playwright errors
            # A browser failure is a retryable failure of this attempt, but it
            # does not blame the slot: relaunching the browser is cheap and the
            # slot itself is almost never the problem.
            raise RetryableError(f"web step {index} ({op}) failed: {exc}") from exc

        raise FatalError(f"unknown web op: {op!r}")

    # -- auth -------------------------------------------------------------

    async def _auth(
        self, driver: Any, step: dict[str, Any], ctx: TaskContext
    ) -> dict[str, Any]:
        """Attach identity to this browser session.

        Returns `reused`, which is the number the operator actually cares about:
        it is the difference between one login per fleet and one login per task.
        """

        mode = str(step.get("mode", "cookie"))
        realm = str(step.get("realm") or step.get("credential") or "default")

        if mode == "cookie":
            if self._cookies.live(realm):
                await driver.set_cookies(self._cookies.load(realm))
                await ctx.progress("auth", "restored session", realm=realm, reused=True)
                return {"auth": {"mode": mode, "realm": realm, "reused": True}}
            # No live session. The task's own steps do the login; this step only
            # reports that they have to, so a spec can branch on it.
            await ctx.progress("auth", "no stored session", realm=realm, reused=False)
            return {"auth": {"mode": mode, "realm": realm, "reused": False}}

        credential = self._resolve(step)

        if mode == "basic":
            name, value = credential.header()
            await driver.set_headers({name: value})
            await ctx.progress("auth", "basic auth attached", realm=realm)
            return {"auth": {"mode": mode, "realm": realm, "reused": False}}

        if mode == "bearer":
            static = credential.secret
            if static is None:
                raise AuthError(f"credential {credential.ref} has no token")

            async def refresh() -> str:
                # The seam a real deployment replaces with a call to the token
                # endpoint. What matters architecturally is where it is called
                # from -- inside the cache, under the per-realm lock -- not what
                # it does, and that does not change when it becomes a request.
                return static.reveal()

            token: Secret = await self._tokens.get(realm, refresh)
            await driver.set_headers({"Authorization": f"Bearer {token.reveal()}"})
            await ctx.progress("auth", "bearer token attached", realm=realm)
            return {"auth": {"mode": mode, "realm": realm, "reused": True}}

        raise FatalError(f"unknown auth mode: {mode!r}")

    async def _capture_session(
        self, driver: Any, step: dict[str, Any], ctx: TaskContext
    ) -> dict[str, Any]:
        """Persist what a successful login produced, for the tasks behind it."""

        realm = str(step.get("realm", "default"))
        cookies = await driver.cookies()
        self._cookies.save(realm, cookies)

        captured: dict[str, Any] = {"cookies": len(cookies)}
        storage_key = step.get("token_key")
        if storage_key:
            raw = await driver.read_storage(str(storage_key))
            if raw:
                # Stored as a Secret and summarised by its expiry. The token
                # itself never reaches the result dict, which is returned over
                # HTTP and written to the log.
                await self._tokens.get(realm, lambda: _identity(raw))
                captured["token_expires_in_s"] = _ttl_of(raw)

        await ctx.progress("auth", "session captured", realm=realm, cookies=len(cookies))
        return {"session": captured}

    def _resolve(self, step: dict[str, Any]) -> Any:
        ref = step.get("credential")
        if not ref:
            raise FatalError("auth step needs a credential reference")
        if self._credentials is None:
            raise FatalError("this deployment has no credential store configured")
        return self._credentials.resolve(str(ref))


async def _identity(value: str) -> str:
    return value


def _ttl_of(token: str) -> Optional[int]:
    from core.auth import jwt_expiry

    try:
        expires_at = jwt_expiry(token)
    except AuthError:
        return None
    return round(expires_at - time.time()) if expires_at else None


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
