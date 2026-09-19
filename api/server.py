"""FastAPI surface over the scheduler.

The HTTP layer owns no state. It holds a reference to an `Orchestrator` that was
assembled in `core/orchestrator.py` and translates between JSON and the core
types. Everything it can do, `cli.py` can also do without it -- and this module
is the only one in the project that imports FastAPI, which is what makes that
claim checkable rather than aspirational.
"""

from __future__ import annotations

import asyncio
import contextlib
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, WebSocket
from fastapi.responses import HTMLResponse, JSONResponse

from api.ws import pump_websocket
from core.orchestrator import Orchestrator
from core.task import FatalError, TaskSpec


def create_app(orchestrator: Orchestrator) -> FastAPI:
    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        await orchestrator.start()
        try:
            yield
        finally:
            await orchestrator.stop()

    app = FastAPI(title="device-orchestrator", version="0.1.0", lifespan=lifespan)

    @app.get("/", response_class=HTMLResponse)
    async def dashboard() -> str:
        # Read per request rather than cached at import, so editing the page
        # during a demo is a browser refresh rather than a restart.
        return (Path(__file__).with_name("dashboard.html")).read_text(encoding="utf-8")

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
            # The dashboard renders its controls from this, rather than assuming
            # they exist and discovering otherwise on a 404 after a click.
            "demo_controls": orchestrator.demo is not None,
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

    def _demo() -> Any:
        if orchestrator.demo is None:
            # Not 403: against a real fleet these routes genuinely do not exist,
            # and saying "forbidden" would imply a deployment where they might.
            raise HTTPException(status_code=404, detail="no demo fleet in this deployment")
        return orchestrator.demo

    @app.post("/demo/scenario", status_code=202)
    async def demo_scenario() -> dict[str, Any]:
        specs = _demo().scenario()
        records = [orchestrator.scheduler.submit(spec) for spec in specs]
        return {"submitted": [r.spec.id for r in records]}

    @app.post("/demo/devices/{device_id}/darken")
    async def demo_darken(device_id: str) -> dict[str, Any]:
        _demo().darken(device_id)
        # Nothing else happens here on purpose. The device is not marked unwell,
        # no event is published, no session is torn down -- it simply stops
        # answering, which is exactly what a phone that has come off its cable
        # does. Everything after this is the system noticing on its own.
        return {"device_id": device_id, "answering": False}

    @app.post("/demo/devices/{device_id}/revive")
    async def demo_revive(device_id: str) -> dict[str, Any]:
        _demo().revive(device_id)
        return {"device_id": device_id, "answering": True}

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
