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
    def _app(self, demo: object | None):
        from api.server import create_app
        from api.ws import EventHub
        from core.device import StaticDeviceSource
        from core.orchestrator import Orchestrator

        return create_app(
            Orchestrator(
                source=StaticDeviceSource(phones("p1")),
                targets={},
                hub=EventHub(),
                demo=demo,
            )
        )

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

        app = self._app(None)
        handler = next(
            r.endpoint for r in app.routes if getattr(r, "path", None) == "/demo/scenario"
        )

        # Registered but inert: with no demo fleet behind them the routes 404,
        # so no wiring accident can expose a button that kills a real phone.
        with self.assertRaises(HTTPException) as caught:
            await handler()
        self.assertEqual(caught.exception.status_code, 404)


if __name__ == "__main__":
    unittest.main()
