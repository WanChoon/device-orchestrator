"""The event hub and the WebSocket fan-out.

`core/` knows nothing about WebSockets. It publishes dicts into an `EventHub`
through a plain `async def sink(event)` callable, and the hub is what happens to
be wired to sockets here. That keeps the scheduler runnable from the CLI with no
web server at all, and it means a second consumer (a file writer, a metrics
exporter) is a subscriber, not a change to core.

Back-pressure policy: a subscriber that cannot keep up loses its oldest events,
not the publisher's progress. A stalled browser tab must never be able to stall
the fleet, so each subscriber has a bounded queue and drops on overflow.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections import deque
from typing import Any, AsyncIterator, Optional

from obs.log import get_logger

log = get_logger("api.ws")


class Subscriber:
    def __init__(self, maxsize: int = 256) -> None:
        self.queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=maxsize)
        self.dropped = 0

    def offer(self, event: dict[str, Any]) -> None:
        try:
            self.queue.put_nowait(event)
        except asyncio.QueueFull:
            # Drop the oldest, keep the newest: for a progress feed, recent
            # state is worth more than complete history, and history is in the
            # JSON log anyway.
            with contextlib.suppress(asyncio.QueueEmpty):
                self.queue.get_nowait()
            self.dropped += 1
            with contextlib.suppress(asyncio.QueueFull):
                self.queue.put_nowait(event)


class EventHub:
    def __init__(self, replay: int = 100) -> None:
        self._subscribers: set[Subscriber] = set()
        # A late-joining dashboard should not see an empty screen while a run is
        # already in flight, so a small ring buffer is replayed on connect.
        self._recent: deque[dict[str, Any]] = deque(maxlen=replay)

    async def publish(self, event: dict[str, Any]) -> None:
        self._recent.append(event)
        for subscriber in list(self._subscribers):
            subscriber.offer(event)

    def recent(self) -> list[dict[str, Any]]:
        return list(self._recent)

    @contextlib.contextmanager
    def subscribe(self, maxsize: int = 256):
        subscriber = Subscriber(maxsize=maxsize)
        self._subscribers.add(subscriber)
        log.info("ws.subscribed", subscribers=len(self._subscribers))
        try:
            yield subscriber
        finally:
            self._subscribers.discard(subscriber)
            log.info(
                "ws.unsubscribed",
                subscribers=len(self._subscribers),
                dropped=subscriber.dropped,
            )

    async def stream(
        self, subscriber: Subscriber, heartbeat_s: float = 20.0
    ) -> AsyncIterator[dict[str, Any]]:
        """Yield events, emitting a heartbeat when idle.

        Without the heartbeat an idle fleet looks identical to a dead server to
        anything sitting behind a proxy that reaps quiet connections.
        """
        while True:
            try:
                event = await asyncio.wait_for(subscriber.queue.get(), timeout=heartbeat_s)
            except asyncio.TimeoutError:
                yield {"type": "heartbeat"}
                continue
            yield event

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)


async def pump_websocket(hub: EventHub, websocket: Any, replay: bool = True) -> None:
    """Drive one WebSocket connection from the hub until the client goes away."""
    with hub.subscribe() as subscriber:
        if replay:
            for event in hub.recent():
                await websocket.send_json(event)
        try:
            async for event in hub.stream(subscriber):
                await websocket.send_json(event)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - a client disconnect is normal
            log.info("ws.closed", reason=str(exc))
