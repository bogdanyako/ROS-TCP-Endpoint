#  Copyright 2020 Unity Technologies
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""Bounded per-connection buffering for complete Unity wire frames."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from threading import Condition
from time import monotonic


@dataclass(frozen=True)
class OutgoingFrame:
    """One wire frame or an indivisible service header and payload."""

    data: bytes
    replaceable_topic: str | None = None
    service_id: int | None = None
    deadline: float | None = None


class OutgoingQueue:
    """Keep reliable items in order and bound waiting items and bytes."""

    def __init__(self, max_items, max_bytes):
        self.max_items = max_items
        self.max_bytes = max_bytes
        self.items = deque()
        self.queued_bytes = 0
        self.closed = False
        self.ready = Condition()

    def put(self, frame):
        """Enqueue without waiting, evicting replaceable state under pressure."""
        size = len(frame.data)
        with self.ready:
            if self.closed or size > self.max_bytes:
                return False

            if frame.replaceable_topic is not None:
                for previous in self.items:
                    if previous.replaceable_topic == frame.replaceable_topic:
                        self._remove(previous)
                        break

            while len(self.items) >= self.max_items or self.queued_bytes + size > self.max_bytes:
                oldest_state = next(
                    (item for item in self.items if item.replaceable_topic is not None), None
                )
                if oldest_state is None:
                    return False
                self._remove(oldest_state)

            # Append replacements at the tail so newer state cannot move ahead of a command.
            self.items.append(frame)
            self.queued_bytes += size
            self.ready.notify()
            return True

    def get(self):
        """Wait for a frame or closure, discarding requests past their deadline."""
        with self.ready:
            while True:
                self.ready.wait_for(lambda: self.closed or self.items)
                if self.closed:
                    return None
                frame = self.items.popleft()
                self.queued_bytes -= len(frame.data)
                if frame.deadline is None or monotonic() < frame.deadline:
                    return frame

    def discard_request(self, service_id):
        """Remove a request that is still waiting in the queue."""
        with self.ready:
            for frame in self.items:
                if frame.service_id == service_id:
                    self._remove(frame)
                    break

    def close(self):
        """Discard queued traffic and wake any waiting writer."""
        with self.ready:
            self.closed = True
            self.items.clear()
            self.queued_bytes = 0
            self.ready.notify_all()

    def _remove(self, frame):
        self.items.remove(frame)
        self.queued_bytes -= len(frame.data)
