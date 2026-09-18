"""Structured JSON logging with a correlation id that follows the task, not the thread.

Every line is one JSON object on stderr. There is no human-readable mode: a fleet
that runs unattended overnight is read by grep and jq, not by eyes, and a format
that is pleasant at 3 devices is unparseable at 30.
"""

from __future__ import annotations

import contextvars
import json
import logging
import sys
import time
import uuid
from contextlib import contextmanager
from typing import Any, Iterator

# The correlation id is stored in a ContextVar rather than passed down every call
# because asyncio tasks inherit context automatically: a task spawned inside a
# scheduler worker keeps the correlation id of the work item that spawned it,
# including across `await`s that hop between coroutines.
_context: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar(
    "orchestrator_log_context", default={}
)

_RESERVED = frozenset(
    {
        "args", "asctime", "created", "exc_info", "exc_text", "filename",
        "funcName", "levelname", "levelno", "lineno", "module", "msecs",
        "message", "msg", "name", "pathname", "process", "processName",
        "relativeCreated", "stack_info", "thread", "threadName", "taskName",
    }
)


def new_correlation_id(prefix: str = "cid") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


@contextmanager
def log_context(**fields: Any) -> Iterator[None]:
    """Bind fields onto every log line emitted inside this block."""
    merged = {**_context.get(), **{k: v for k, v in fields.items() if v is not None}}
    token = _context.set(merged)
    try:
        yield
    finally:
        _context.reset(token)


def current_context() -> dict[str, Any]:
    return dict(_context.get())


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
            "level": record.levelname.lower(),
            "event": record.getMessage(),
            "logger": record.name,
        }
        payload.update(_context.get())
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["error"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=_fallback, ensure_ascii=False)


def _fallback(value: Any) -> str:
    return repr(value)


class StructuredLogger:
    """Thin wrapper so call sites read `log.info("task.started", task_id=...)`.

    The first argument is an event name, not a sentence. Event names are stable
    identifiers you can alert on; sentences are not.
    """

    def __init__(self, name: str) -> None:
        self._logger = logging.getLogger(name)

    def _emit(self, level: int, event: str, **fields: Any) -> None:
        clean = {k: v for k, v in fields.items() if k not in _RESERVED}
        self._logger.log(level, event, extra=clean)

    def debug(self, event: str, **fields: Any) -> None:
        self._emit(logging.DEBUG, event, **fields)

    def info(self, event: str, **fields: Any) -> None:
        self._emit(logging.INFO, event, **fields)

    def warning(self, event: str, **fields: Any) -> None:
        self._emit(logging.WARNING, event, **fields)

    def error(self, event: str, exc_info: bool = False, **fields: Any) -> None:
        clean = {k: v for k, v in fields.items() if k not in _RESERVED}
        self._logger.error(event, exc_info=exc_info, extra=clean)


def configure(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())
    # uvicorn installs its own colourised handlers; force them through ours so a
    # served run produces one parseable stream rather than two interleaved ones.
    for noisy in ("uvicorn", "uvicorn.access", "uvicorn.error"):
        logger = logging.getLogger(noisy)
        logger.handlers[:] = []
        logger.propagate = True


def get_logger(name: str) -> StructuredLogger:
    return StructuredLogger(name)
