"""Tests for the fleet console and the controls that drive it.

The FastAPI-dependent assertions are skipped when FastAPI is absent, because the
project's other guarantee is that everything except `serve` runs on a bare
standard library, and a test file that imports a web framework unconditionally
would quietly break it.

    python -m unittest discover -s tests -v
"""

from __future__ import annotations

import importlib.util
import re
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cli import DemoHooks  # noqa: E402
from core.device import Device  # noqa: E402
from core.task import TaskState  # noqa: E402
from obs.log import configure  # noqa: E402

configure("CRITICAL")

DASHBOARD = Path(__file__).resolve().parents[1] / "api" / "dashboard.html"

# `find_spec` rather than a try-import: asking whether FastAPI is installed
# should not be the thing that installs it in `sys.modules`, because test
# discovery then looks like a hard dependency to anything checking for one.
HAS_FASTAPI = importlib.util.find_spec("fastapi") is not None


def phones(*ids: str) -> list[Device]:
    return [Device(id=i, transport="usb", model="test", tags=("android",)) for i in ids]


class DemoHookTests(unittest.TestCase):
    def test_darkening_is_shared_with_the_drivers(self) -> None:
        dark: set[str] = set()
        hooks = DemoHooks(dark, phones("p1", "p2"), browsers=1)

        # The set the route mutates is the set the fake driver reads. If these
        # ever became two collections the demo would show a device that the
        # health monitor thinks is fine, which is the opposite of the point.
        hooks.darken("p1")
        self.assertIn("p1", dark)
        hooks.revive("p1")
        self.assertNotIn("p1", dark)

    def test_silencing_is_reported_separately_from_health(self) -> None:
        dark: set[str] = set()
        hooks = DemoHooks(dark, phones("p1", "p2"), browsers=1)

        self.assertEqual(hooks.not_answering(), [])
        hooks.darken("p2")
        # "This phone stopped answering" is a fact about the world. "This phone
        # is quarantined" is a conclusion the system reaches later, on its own.
        # Collapsing the two is what made the console look broken: the click
        # had no visible effect until the health monitor caught up.
        self.assertEqual(hooks.not_answering(), ["p2"])
        hooks.revive("p2")
        self.assertEqual(hooks.not_answering(), [])

    def test_reviving_something_already_alive_is_not_an_error(self) -> None:
        dark: set[str] = set()
        DemoHooks(dark, phones("p1"), browsers=1).revive("p1")
        self.assertEqual(dark, set())

    def test_the_scenario_includes_work_that_can_never_be_scheduled(self) -> None:
        specs = DemoHooks(set(), phones("p1", "p2"), browsers=2).scenario()
        kinds = {spec.kind for spec in specs}
        self.assertEqual(kinds, {"android", "web"})

        # "Queued for a long time" and "impossible" look identical on a
        # dashboard until something distinguishes them, so the scripted run
        # always contains one task no device can satisfy.
        unschedulable = [s for s in specs if "ios" in s.selector.tags]
        self.assertEqual(len(unschedulable), 1)
        self.assertIs(TaskState.PENDING, TaskState("pending"))

    def test_the_scenario_scales_with_the_fleet(self) -> None:
        small = DemoHooks(set(), phones("p1"), browsers=1).scenario()
        large = DemoHooks(set(), phones("p1", "p2", "p3"), browsers=1).scenario()
        self.assertGreater(len(large), len(small))


class DashboardFileTests(unittest.TestCase):
    """The console is a file on disk the server reads, so it is testable as one."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.html = DASHBOARD.read_text(encoding="utf-8")

    def test_it_exists_and_is_a_document(self) -> None:
        self.assertTrue(DASHBOARD.is_file())
        self.assertIn("<!doctype html>", self.html.lower())

    def test_it_talks_to_the_endpoints_that_exist(self) -> None:
        for endpoint in ("/ws/progress", "healthz", "devices", "tasks", "demo/scenario"):
            self.assertIn(endpoint, self.html, f"console never calls {endpoint}")

    def test_it_separates_being_silent_from_being_diagnosed(self) -> None:
        # The console must render the gap between "stopped answering" and
        # "the system noticed", because during that gap a silenced phone is
        # still ONLINE and still assignable, and a page that shows nothing
        # there is indistinguishable from a page whose button is broken.
        self.assertIn("not_answering", self.html)
        self.assertIn("has not noticed yet", self.html)

    def test_it_acknowledges_a_click_before_the_round_trip(self) -> None:
        self.assertIn("silencedAt.set", self.html)

    def test_it_loads_nothing_from_the_network(self) -> None:
        # An operator console that pulls a font or a framework from a CDN is
        # broken on exactly the bench that needs it most: an isolated one. This
        # asserts the page is genuinely self-contained.
        external = re.findall(r'(?:src|href)\s*=\s*["\'](https?:)?//[^"\']+', self.html)
        self.assertEqual(external, [], f"console loads external resources: {external}")

    def test_it_admits_when_the_feed_is_incomplete(self) -> None:
        # The hub drops oldest-first under pressure. A console that renders a
        # lossy feed as though it were complete is worse than no console.
        self.assertIn("dropped", self.html)


@unittest.skipUnless(HAS_FASTAPI, "fastapi is only needed by `serve`")
class RouteTests(unittest.IsolatedAsyncioTestCase):
    def _build(self, demo: object | None):
        from api.server import create_app
        from api.ws import EventHub
        from core.device import StaticDeviceSource
        from core.orchestrator import Orchestrator

        orchestrator = Orchestrator(
            source=StaticDeviceSource(phones("p1")),
            targets={},
            hub=EventHub(),
            demo=demo,
        )
        return orchestrator, create_app(orchestrator)

    def _app(self, demo: object | None):
        return self._build(demo)[1]

    @staticmethod
    def _route(app, path: str):
        return next(r.endpoint for r in app.routes if getattr(r, "path", None) == path)

    async def test_devices_reports_what_has_been_silenced(self) -> None:
        hooks = DemoHooks(set(), phones("p1"), 1)
        orchestrator, app = self._build(hooks)
        # The inventory is loaded by `start()`; without it the registry is
        # empty and the state assertion below would pass vacuously.
        await orchestrator.registry.refresh()
        handler = self._route(app, "/devices")

        self.assertEqual((await handler())["not_answering"], [])
        hooks.darken("p1")
        payload = await handler()
        self.assertEqual(payload["not_answering"], ["p1"])
        # Still online: nothing about darkening touches device health, which is
        # the property the console is built to show.
        self.assertEqual(payload["devices"][0]["state"], "online")

    async def test_a_real_deployment_reports_nothing_silenced(self) -> None:
        handler = self._route(self._app(None), "/devices")
        self.assertEqual((await handler())["not_answering"], [])

    def test_the_console_and_demo_routes_are_registered(self) -> None:
        paths = {r.path for r in self._app(DemoHooks(set(), phones("p1"), 1)).routes}
        for path in (
            "/",
            "/healthz",
            "/devices",
            "/tasks",
            "/demo/scenario",
            "/demo/devices/{device_id}/darken",
            "/demo/devices/{device_id}/revive",
            "/ws/progress",
        ):
            self.assertIn(path, paths)

    async def test_a_real_deployment_gets_no_kill_switch(self) -> None:
        from fastapi import HTTPException

        handler = self._route(self._app(None), "/demo/scenario")

        # Registered but inert: with no demo fleet behind them the routes 404,
        # so no wiring accident can expose a button that kills a real phone.
        with self.assertRaises(HTTPException) as caught:
            await handler()
        self.assertEqual(caught.exception.status_code, 404)


if __name__ == "__main__":
    unittest.main()
