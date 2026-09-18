"""The Task contract.

This module is the load-bearing one. Everything else in `core/` is written
against the types here, and every target in `targets/` implements `Target`.
The scheduler never imports a target module; it only ever holds a `Target`.

The contract is deliberately narrow:

    outcome = await target.execute(spec, lease, ctx)

with exactly three ways to finish:

    return            -> succeeded, the dict is the result
    RetryableError    -> the attempt failed, the work item is still valid
    FatalError        -> the work item itself is wrong, do not retry it anywhere

Anything else that escapes (including cancellation from the deadline the
scheduler enforces) is treated as retryable but *also* as evidence against the
device, because an unclassified failure is exactly what a wedged device
produces.
"""

from __future__ import annotations

import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable, ClassVar, Optional

from obs.log import StructuredLogger


class TaskState(str, Enum):
    PENDING = "pending"        # queued, no device yet
    ASSIGNED = "assigned"      # holds a lease, not started
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"          # retries exhausted
    REJECTED = "rejected"      # FatalError: never retried
    ABANDONED = "abandoned"    # no device could ever satisfy the selector


TERMINAL_STATES = frozenset(
    {TaskState.SUCCEEDED, TaskState.FAILED, TaskState.REJECTED, TaskState.ABANDONED}
)


class TaskError(Exception):
    """Base for errors a target raises deliberately."""


class RetryableError(TaskError):
    """The attempt failed; the work item is still valid and may run again.

    `blames_device` separates "the app showed an unexpected screen" from "the
    device stopped answering". Only the latter counts towards quarantining
    hardware, so a buggy script cannot take a healthy phone out of the pool.
    """

    def __init__(self, message: str, *, blames_device: bool = False) -> None:
        super().__init__(message)
        self.blames_device = blames_device


class FatalError(TaskError):
    """The work item is malformed or semantically impossible. Never retried."""


@dataclass(frozen=True)
class DeviceSelector:
    """What a task needs from a device. Empty means any healthy device."""

    transports: tuple[str, ...] = ()   # e.g. ("usb", "tunnel")
    tags: tuple[str, ...] = ()         # all must be present on the device
    device_id: Optional[str] = None    # pin to one device

    @classmethod
    def from_dict(cls, raw: Optional[dict[str, Any]]) -> "DeviceSelector":
        raw = raw or {}
        return cls(
            transports=tuple(raw.get("transports", ())),
            tags=tuple(raw.get("tags", ())),
            device_id=raw.get("device_id"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "transports": list(self.transports),
            "tags": list(self.tags),
            "device_id": self.device_id,
        }


def _new_task_id() -> str:
    return "task-" + uuid.uuid4().hex[:10]


@dataclass(frozen=True)
class TaskSpec:
    """An immutable unit of work. Serialisable: this is what crosses the API."""

    kind: str                                  # "android" | "web"
    steps: tuple[dict[str, Any], ...]
    id: str = field(default_factory=_new_task_id)
    selector: DeviceSelector = field(default_factory=DeviceSelector)
    timeout_s: float = 120.0
    max_attempts: int = 3
    params: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "TaskSpec":
        kind = raw.get("kind")
        if not kind:
            raise FatalError("task.kind is required")
        steps = raw.get("steps") or []
        if not isinstance(steps, list):
            raise FatalError("task.steps must be a list")
        return cls(
            kind=str(kind),
            steps=tuple(steps),
            id=str(raw["id"]) if raw.get("id") else _new_task_id(),
            selector=DeviceSelector.from_dict(raw.get("selector")),
            timeout_s=float(raw.get("timeout_s", 120.0)),
            max_attempts=int(raw.get("max_attempts", 3)),
            params=dict(raw.get("params") or {}),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "kind": self.kind,
            "steps": list(self.steps),
            "selector": self.selector.to_dict(),
            "timeout_s": self.timeout_s,
            "max_attempts": self.max_attempts,
            "params": self.params,
        }


@dataclass
class Attempt:
    number: int
    device_id: Optional[str] = None
    started_at: float = field(default_factory=time.time)
    ended_at: Optional[float] = None
    error: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "number": self.number,
            "device_id": self.device_id,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "duration_s": (self.ended_at - self.started_at) if self.ended_at else None,
            "error": self.error,
        }


@dataclass
class TaskRecord:
    """Mutable server-side state for one submitted spec."""

    spec: TaskSpec
    correlation_id: str
    state: TaskState = TaskState.PENDING
    attempts: list[Attempt] = field(default_factory=list)
    result: Optional[dict[str, Any]] = None
    error: Optional[str] = None
    submitted_at: float = field(default_factory=time.time)

    @property
    def attempt_count(self) -> int:
        return len(self.attempts)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.spec.id,
            "kind": self.spec.kind,
            "state": self.state.value,
            "correlation_id": self.correlation_id,
            "attempts": [a.to_dict() for a in self.attempts],
            "result": self.result,
            "error": self.error,
            "submitted_at": self.submitted_at,
        }


ProgressSink = Callable[[dict[str, Any]], Awaitable[None]]


@dataclass
class TaskContext:
    """Everything a target is allowed to know about its surroundings.

    A target gets a logger, a progress channel and a deadline -- never the
    registry, the scheduler or the event hub. That is what keeps a target
    testable without standing up the rest of the system.
    """

    task_id: str
    correlation_id: str
    attempt: int
    log: StructuredLogger
    deadline: float
    device_id: Optional[str] = None
    sink: Optional[ProgressSink] = None

    def remaining_s(self) -> float:
        return max(0.0, self.deadline - time.monotonic())

    async def progress(self, phase: str, message: str = "", **data: Any) -> None:
        event = {
            "type": "task.progress",
            "task_id": self.task_id,
            "correlation_id": self.correlation_id,
            "device_id": self.device_id,
            "attempt": self.attempt,
            "phase": phase,
            "message": message,
            "data": data,
            "ts": time.time(),
        }
        self.log.info("task.progress", phase=phase, message=message, **data)
        if self.sink is not None:
            await self.sink(event)


class Target(ABC):
    """One automation backend. Android and web are peers, not special cases."""

    kind: ClassVar[str] = ""

    @abstractmethod
    async def execute(self, spec: TaskSpec, lease: Any, ctx: TaskContext) -> dict[str, Any]:
        """Run `spec` against the device held by `lease`.

        Must return a JSON-serialisable dict, or raise RetryableError /
        FatalError. Must be cancellation-safe: the scheduler enforces
        `spec.timeout_s` by cancelling this coroutine, so clean-up belongs in a
        `finally` block.
        """

    async def close(self) -> None:
        """Release any long-lived resources. Called once at shutdown."""
