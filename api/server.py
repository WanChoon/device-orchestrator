"""FastAPI surface over the scheduler.

The HTTP layer owns no state. It holds a reference to an `Orchestrator` that was
assembled elsewhere and translates between JSON and the core types. Everything
it can do, `cli.py` can also do without it -- which is the test that the split
is real rather than decorative.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, WebSocket
from fastapi.responses import JSONResponse

from api.ws import EventHub, pump_websocket
from core.device import Adb, DeviceRegistry, DeviceSource
from core.health import HealthMonitor, HealthPolicy
from core.scheduler import Scheduler, SchedulerPolicy
from core.session import SessionManager
from core.task import FatalError, Target, TaskSpec
from obs.log import get_logger

log = get_logger("api.server")


class Orchestrator:
    """Assembles the parts and owns their lifecycle.

    This is the only place that knows the wiring. Tests build one of these with
    fake sources and fake factories; production builds one with adb and Appium.
    Nothing downstream has to care which.
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


def create_app(orchestrator: Orchestrator) -> FastAPI:
    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        await orchestrator.start()
        try:
            yield
        finally:
            await orchestrator.stop()

    app = FastAPI(title="device-orchestrator", version="0.1.0", lifespan=lifespan)

    @app.get("/healthz")
    async def healthz() -> dict[str, Any]:
        devices = orchestrator.registry.snapshot()
        assignable = [d for d in devices if d["state"] == "online"]
        return {
            # Liveness is not readiness: the process can be perfectly healthy
            # and still have nothing to run work on, and a deploy pipeline needs
            # to tell those apart.
            "status": "ok",
            "ready": bool(assignable),
            "devices_total": len(devices),
            "devices_online": len(assignable),
            "scheduler": orchestrator.scheduler.stats(),
            "ws_subscribers": orchestrator.hub.subscriber_count,
        }

    @app.get("/devices")
    async def list_devices() -> dict[str, Any]:
        return {
            "devices": orchestrator.registry.snapshot(),
            "health": orchestrator.health.snapshot(),
            "sessions": orchestrator.sessions.describe(),
        }

    @app.post("/devices/refresh")
    async def refresh_devices() -> dict[str, Any]:
        await orchestrator.registry.refresh()
        return {"devices": orchestrator.registry.snapshot()}

    @app.post("/tasks", status_code=202)
    async def submit_task(payload: dict[str, Any]) -> JSONResponse:
        try:
            spec = TaskSpec.from_dict(payload)
        except FatalError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        record = orchestrator.scheduler.submit(spec)
        return JSONResponse(status_code=202, content=record.to_dict())

    @app.get("/tasks")
    async def list_tasks() -> dict[str, Any]:
        return {"tasks": [r.to_dict() for r in orchestrator.scheduler.records()]}

    @app.get("/tasks/{task_id}")
    async def get_task(task_id: str) -> dict[str, Any]:
        record = orchestrator.scheduler.get(task_id)
        if record is None:
            raise HTTPException(status_code=404, detail="no such task")
        return record.to_dict()

    @app.websocket("/ws/progress")
    async def progress_socket(websocket: WebSocket) -> None:
        await websocket.accept()
        # The client never sends anything; a reader task exists only so a
        # half-open connection is noticed instead of leaking a subscriber.
        reader = asyncio.create_task(_drain(websocket))
        try:
            await pump_websocket(orchestrator.hub, websocket)
        finally:
            reader.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await reader

    return app


async def _drain(websocket: WebSocket) -> None:
    with contextlib.suppress(Exception):
        while True:
            await websocket.receive_text()
