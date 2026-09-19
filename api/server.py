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
from typing import Any

from fastapi import FastAPI, HTTPException, WebSocket
from fastapi.responses import JSONResponse

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
