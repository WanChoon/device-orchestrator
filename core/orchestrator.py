"""Assembly: the one place that knows how the parts are wired together.

This lives in `core/` rather than next to the HTTP routes for a reason that is
easy to state and easy to get wrong. The README claims that everything the API
can do, the CLI can do without it -- that the split between orchestration and its
HTTP surface is real rather than decorative. While this class lived in
`api/server.py`, that claim was false in the most mechanical way possible:
`python cli.py demo` could not import, let alone run, on a machine without
FastAPI installed. CI caught it on the first run.

So the dependency points the right way now. `core/` and `targets/` need nothing
but the standard library; FastAPI and uvicorn are needed only by `serve`. A
reviewer can clone this repo, install nothing at all, and watch the failure path
they came to see.
"""

from __future__ import annotations

from typing import Any, Optional

from api.ws import EventHub
from core.device import Adb, DeviceRegistry, DeviceSource
from core.health import HealthMonitor, HealthPolicy
from core.scheduler import Scheduler, SchedulerPolicy
from core.session import SessionManager
from core.task import Target
from obs.log import get_logger

log = get_logger("core.orchestrator")


class Orchestrator:
    """Assembles the parts and owns their lifecycle.

    Tests build one of these with fake sources and fake factories; production
    builds one with adb, Appium and a browser. Nothing downstream has to care
    which, and that is the only reason the fakes are worth having.
    """

    def __init__(
        self,
        *,
        source: DeviceSource,
        targets: dict[str, Target],
        adb: Optional[Adb] = None,
        sessions: Optional[SessionManager] = None,
        scheduler_policy: Optional[SchedulerPolicy] = None,
        health_policy: Optional[HealthPolicy] = None,
        hub: Optional[EventHub] = None,
        probe: Optional[Any] = None,
    ) -> None:
        self.hub = hub or EventHub()
        self.registry = DeviceRegistry(source)
        self.sessions = sessions or SessionManager()
        self.adb = adb or Adb()
        self.health = HealthMonitor(
            self.registry,
            self.adb,
            self.sessions,
            policy=health_policy,
            sink=self.hub.publish,
            probe=probe,
        )
        self.scheduler = Scheduler(
            self.registry,
            targets,
            policy=scheduler_policy,
            sink=self.hub.publish,
            health=self.health,
        )
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        # Refresh before the workers start so the first task does not wait a
        # full health interval to discover that devices exist.
        await self.registry.refresh()
        await self.health.start()
        await self.scheduler.start()
        self._started = True
        log.info("orchestrator.started", devices=len(self.registry.all()))

    async def stop(self) -> None:
        if not self._started:
            return
        await self.scheduler.stop()
        await self.health.stop()
        await self.sessions.close_all()
        self._started = False
        log.info("orchestrator.stopped")
