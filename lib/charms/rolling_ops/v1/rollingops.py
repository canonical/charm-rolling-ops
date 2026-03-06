# Copyright 2026 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Rolling Ops v1 — coordinated rolling operations for Juju charms.

This library provides a reusable mechanism for coordinating rolling operations
across units of a Juju application using a peer-relation distributed lock.

The library guarantees that at most one unit executes a rolling operation at any
time, while allowing multiple units to enqueue operations and participate
in a coordinated rollout.

## Data model (peer relation)

### Unit databag

Each unit maintains a FIFO queue of operations it wishes to execute.

Keys:
- `operations`: JSON-encoded list of queued `Operation` objects
- `state`: `"idle"` | `"request"` | `"retry"`
- `executed_at`: UTC timestamp string indicating when the current operation last ran

Each `Operation` contains:
- `callback_id`: identifier of the callback to execute
- `kwargs`: JSON-serializable arguments for the callback
- `requested_at`: UTC timestamp when the operation was enqueued
- `max_retry`: maximum retry count (`< 0` means unlimited)
- `attempt`: current attempt number

### Application databag

The application databag represents the global lock state.

Keys:
- `granted_unit`: unit identifier (unit name), or empty
- `granted_at`: UTC timestamp indicating when the lock was granted

## Operation semantics

- Units enqueue operations instead of overwriting a single pending request.
- Duplicate operations (same `callback_id` and `kwargs`) are ignored if they are
  already the last queued operation.
- When granted the lock, a unit executes exactly one operation (the queue head).
- After execution, the lock is released so that other units may proceed.

## Retry semantics

- If a callback returns `OperationResult.RETRY_RELEASE` the unit will release the
lock and retry the operation later.
- If a callback return `OperationResult.RETRY_HOLD` the unit will keep the
lock and retry immediately.
- Retry state (`attempt`) is tracked per operation.
- When `max_retry` is exceeded, the failing operation is dropped and the unit
  proceeds to the next queued operation, if any.

## Scheduling semantics

- Only the leader schedules lock grants.
- If a valid lock grant exists, no new unit is scheduled.
- Requests are preferred over retries.
- Among requests, the operation with the oldest `requested_at` timestamp is selected.
- Among retries, the operation with the oldest `executed_at` timestamp is selected.
- Stale grants (e.g., pointing to departed units) are automatically released.

All timestamps are stored in UTC using `TIMESTAMP_FORMAT`.

## Using the library in a charm

### 1. Declare a peer relation

```yaml
peers:
  restart:
    interface: rolling_op
```

Import this library into src/charm.py, and initialize a RollingOpsManager in the Charm's
`__init__`. The Charm should also define a callback routine, which will be executed when
a unit holds the distributed lock:

src/charm.py
```python
from charms.rolling_ops.v1.rollingops import RollingOpsManagerv1, OperationResult

class SomeCharm(CharmBase):
    def __init__(self, *args):
        super().__init__(*args)

        self.rolling_ops = RollingOpsManagerv1(
            charm=self,
            relation="restart",
            callback_targets={
                "restart": self._restart,
                "failed_restart": self._failed_restart,
                "defer_restart": self._defer_restart,
            },
        )

    def _restart(self, force: bool) -> OperationResult:
        # perform restart logic
        return OperationResult.RELEASE

    def _failed_restart(self) -> OperationResult:
        # perform restart logic
        return OperationResult.RETRY_RELEASE

    def _defer_restart(self) -> OperationResult:
        if not self.ready():
            event.defer()
            return OperationResult.RETRY_HOLD
        # do restart logic
        return OperationResult.RELEASE
```

Request a rolling operation

```python

    def _on_restart_action(self, event):
        self.rolling_ops.request_async_lock(
            callback_id="restart",
            kwargs={"force": True},
            max_retry=3,
    )
```

All participating units must enqueue the operation in order to be included
in the rolling execution.

Units that do not enqueue the operation will be skipped, allowing operators
to recover from partial failures by reissuing requests selectively.
"""

import argparse
import json
import logging
import os
import signal
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from sys import version_info
from typing import Any, Optional

from ops import Relation
from ops.charm import (
    CharmBase,
    RelationChangedEvent,
    RelationDepartedEvent,
)
from ops.framework import EventBase, Object

logger = logging.getLogger(__name__)

# The unique Charmhub library identifier, never change it
LIBID = "20b7777f58fe421e9a223aefc2b4d3a4"

# Increment this major API version when introducing breaking changes
LIBAPI = 1

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 0

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S.%fZ"


def _now_timestamp_str() -> str:
    """UTC timestamp string with microseconds."""
    return datetime.now(timezone.utc).strftime(TIMESTAMP_FORMAT)


def _now_timestamp() -> datetime:
    """UTC timestamp string with microseconds."""
    return datetime.now(timezone.utc)


def _parse_timestamp(timestamp: str) -> Optional[datetime]:
    """Parse timestamp string. Return 'now' on errors to avoid selecting invalid timestamps."""
    try:
        dt = datetime.strptime(timestamp, TIMESTAMP_FORMAT)
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _args_to_json(data: dict[str, Any]) -> str:
    """Deterministic JSON serialization for kwargs."""
    return json.dumps(data, sort_keys=True, separators=(",", ":"))


class LockNoRelationError(Exception):
    """Raised if we are trying to process a lock, but do not appear to have a relation yet."""


@dataclass
class Operation:
    """A single queued operation."""

    callback_id: str
    kwargs: dict[str, Any]
    requested_at: Optional[datetime]
    max_retry: Optional[int]
    attempt: int

    def __post_init__(self) -> None:
        """Vallidate the class attributes."""
        if not isinstance(self.callback_id, str) or not self.callback_id.strip():
            raise ValueError("callback_id must be a non-empty string")

        if not isinstance(self.kwargs, dict):
            raise ValueError("kwargs must be a dict")
        try:
            json.dumps(self.kwargs)
        except TypeError as e:
            raise ValueError(f"kwargs must be JSON-serializable: {e}") from e

        if self.requested_at is not None and not isinstance(self.requested_at, datetime):
            raise ValueError("requested_at must be a datetime or None")

        if self.max_retry:
            if not isinstance(self.max_retry, int):
                raise ValueError("max_retry must be an int")
            if self.max_retry < 0:
                raise ValueError("max_retry must be >= 0")

        if not isinstance(self.attempt, int):
            raise ValueError("attempt must be an int")
        if self.attempt < 0:
            raise ValueError("attempt must be >= 0")

    @classmethod
    def create(
        cls,
        callback_id: str,
        kwargs: dict[str, Any],
        max_retry: int | None = None,
    ) -> "Operation":
        """Create a new operation from a callback id and kwargs."""
        return cls(
            callback_id=callback_id,
            kwargs=kwargs,
            requested_at=_now_timestamp(),
            max_retry=max_retry,
            attempt=0,
        )

    def _to_dict(self) -> dict[str, str]:
        """Dict form (string-only values)."""
        return {
            "callback_id": self.callback_id,
            "kwargs": _args_to_json(self.kwargs),
            "requested_at": self.requested_at.strftime(TIMESTAMP_FORMAT)
            if self.requested_at
            else "",
            "max_retry": str(self.max_retry) if self.max_retry else "",
            "attempt": str(self.attempt),
        }

    def to_string(self) -> str:
        """Serialize to a string suitable for a Juju databag."""
        return json.dumps(self._to_dict(), separators=(",", ":"))

    def increase_attempt(self) -> None:
        """Increment the attempt counter."""
        self.attempt += 1

    def is_max_retry_reached(self) -> bool:
        """Return True if attempt exceeds max_retry (unless max_retry is None)."""
        if not self.max_retry:
            return False
        return self.attempt > self.max_retry

    @classmethod
    def from_string(cls, data: str) -> "Operation":
        """Deserialize from a Juju databag string."""
        obj = json.loads(data)
        return cls(
            callback_id=obj["callback_id"],
            kwargs=json.loads(obj["kwargs"]) if obj.get("kwargs") else {},
            requested_at=_parse_timestamp(obj["requested_at"])
            if obj.get("requested_at")
            else None,
            max_retry=int(obj["max_retry"]) if obj.get("max_retry") else None,
            attempt=int(obj["attempt"]),
        )

    def __eq__(self, other: object) -> bool:
        """Equal for the operation."""
        if not isinstance(other, Operation):
            return NotImplemented
        return self.callback_id == other.callback_id and self.kwargs == other.kwargs

    def __hash__(self) -> int:
        """Hash for the operation."""
        return hash((self.callback_id, _args_to_json(self.kwargs)))


class OperationQueue:
    """In-memory FIFO queue of Operations with encode/decode helpers for storing in a databag."""

    def __init__(self, operations: Optional[list[Operation]] = None):
        self.operations: list[Operation] = list(operations or [])

    def __len__(self) -> int:
        """Return the number of operations in the queue."""
        return len(self.operations)

    def is_empty(self) -> bool:
        """Return True if there are no queued operations."""
        return not self.operations

    def peek(self) -> Optional[Operation]:
        """Return the first operation in the queue if it exists."""
        return self.operations[0] if self.operations else None

    def _peek_last(self) -> Optional[Operation]:
        """Return the last operation in the queue if it exists."""
        return self.operations[-1] if self.operations else None

    def dequeue(self) -> Optional[Operation]:
        """Drop the first operation in the queue if it exists and return it."""
        return self.operations.pop(0) if self.operations else None

    def _enqueue(self, operation: Operation) -> bool:
        """Append operation only if it is not equal to the last enqueued operation.

        Returns True if added, False if it was already in the queue.
        """
        if last_operation := self._peek_last():
            if last_operation == operation:
                return False
        self.operations.append(operation)
        return True

    def increase_attempt(self):
        """Increment the attempt counter for the head operation and persist it."""
        if self.is_empty():
            return
        self.operations[0].increase_attempt()

    def enqueue_lock_request(
        self, callback_id: str, kwargs: dict[str, Any], max_retry: int | None = None
    ) -> bool:
        """Enqueue a lock request."""
        return self._enqueue(Operation.create(callback_id, kwargs, max_retry=max_retry))

    def to_string(self) -> str:
        """Encode entire queue to a single string."""
        items = [op.to_string() for op in self.operations]
        return json.dumps(items, separators=(",", ":"))

    @classmethod
    def from_string(cls, data: str) -> "OperationQueue":
        """Decode queue from a single string."""
        if not data:
            return cls([])
        items = json.loads(data)
        if not isinstance(items, list):
            raise ValueError("Queue string must decode to a JSON list")
        operations = [Operation.from_string(s) for s in items]
        return cls(operations)


class LockIntent(Enum):
    """Unit-level lock intents stored in unit databags."""

    REQUEST = "request"
    RETRY_RELEASE = "retry-release"
    RETRY_HOLD = "retry-hold"
    IDLE = "idle"


class OperationResult(Enum):
    """Callback return values."""

    RELEASE = "release"
    RETRY_RELEASE = "retry-release"
    RETRY_HOLD = "retry-hold"


class Lock:
    """State machine view over peer relation databags for a single unit.

    This class is the only component that should directly read/write the peer relation
    databags for lock state, queue state, and grant state.

    Important:
      - All relation databag values are strings.
      - This class updates both unit databags and app databags, which triggers
        relation-changed events.
    """

    def __init__(self, manager, unit=None):
        self.relation = manager.model.relations[manager.relation_name][0]
        if not self.relation:
            # TODO: defer caller in this case (probably just fired too soon).
            raise LockNoRelationError()

        self.unit = unit or manager.model.unit
        self.app = manager.model.app

    @property
    def _app_data(self):
        return self.relation.data[self.app]

    @property
    def _unit_data(self):
        return self.relation.data[self.unit]

    @property
    def _operations(self) -> OperationQueue:
        return OperationQueue.from_string(self._unit_data.get("operations", ""))

    def request(self, callback_id: str, kwargs: dict, max_retry: int | None = None):
        """Enqueue an operation and mark this unit as requesting the lock.

        Args:
          callback_id: identifies which callback to execute.
          kwargs: dict of callback kwargs.
          max_retry: None -> unlimited retries, else explicit integer.
        """
        queue = self._operations
        if queue.is_empty():
            self._unit_data.update({"state": LockIntent.REQUEST.value})
        if queue.enqueue_lock_request(callback_id, kwargs, max_retry):
            logger.debug("Operation added to the queue.")
        else:
            logger.info("Operation %s not added to queue.")
        self._unit_data.update({"operations": queue.to_string()})

    def _set_retry(self, intent: LockIntent):
        """Mark retry for the head operation.

        If max_retry is reached, the head operation is dropped via complete().
        """
        self._increase_attempt()
        if self._is_max_retry_reached():
            logger.info("Operation max retry reached. Dropping")
            self.complete()
            return
        self._unit_data.update({
            "executed_at": _now_timestamp_str(),
            "state": intent.value,
        })

    def retry_release(self):
        """Mark retry for the head operation.

        If max_retry is reached, the head operation is dropped via complete().
        """
        self._set_retry(LockIntent.RETRY_RELEASE)

    def retry_hold(self):
        """Mark retry for the head operation.

        If max_retry is reached, the head operation is dropped via complete().
        """
        self._set_retry(LockIntent.RETRY_HOLD)

    def complete(self):
        """Mark the head operation as completed successfully, pop it from the queue.

        Update unit state depending on whether more operations remain.
        """
        queue = self._operations
        queue.dequeue()
        next_state = LockIntent.REQUEST.value if queue.peek() else LockIntent.IDLE.value

        self._unit_data.update({
            "state": next_state,
            "operations": queue.to_string(),
            "executed_at": _now_timestamp_str(),
        })

    def release(self):
        """Clear the application-level grant."""
        self._app_data.update({"granted_unit": "", "granted_at": ""})

    def grant(self) -> None:
        """Grant a lock to a unit."""
        self._app_data.update({
            "granted_unit": str(self.unit.name),
            "granted_at": _now_timestamp_str(),
        })

    def is_granted(self) -> bool:
        """Return True if the unit holds the lock."""
        granted_unit = self._app_data.get("granted_unit", "")
        return granted_unit == str(self.unit.name)

    def should_run(self) -> bool:
        """Return True if the lock has been granted to the unit and it is time to execute callback."""
        return self.is_granted() and not self._unit_executed_after_grant()

    def should_release(self) -> bool:
        """Return True if the unit finished executing the callback and should be released."""
        return self.is_completed() or self._unit_executed_after_grant()

    def is_waiting(self) -> bool:
        """Return True if this unit is waiting for a lock to be granted."""
        unit_intent = self._unit_data.get("state")
        return unit_intent == LockIntent.REQUEST.value and not self.is_granted()

    def is_completed(self) -> bool:
        """Return True if this unit is completed callback but still has the grant (leader should clear)."""
        unit_intent = self._unit_data.get("state")
        return unit_intent == LockIntent.IDLE.value and self.is_granted()

    def is_retry(self) -> bool:
        """Return True if this unit requested retry but still has the grant (leader should clear)."""
        unit_intent = self._unit_data.get("state")
        return (
            unit_intent == LockIntent.RETRY_RELEASE.value
            or unit_intent == LockIntent.RETRY_HOLD.value
        ) and self.is_granted()

    def is_waiting_retry(self) -> bool:
        """Return True if the unit requested retry and is waiting for lock to be granted."""
        unit_intent = self._unit_data.get("state")
        return unit_intent == LockIntent.RETRY_RELEASE.value and not self.is_granted()

    def is_retry_hold(self) -> bool:
        """Return True if the unit requested retry and is waiting for lock to be granted."""
        unit_intent = self._unit_data.get("state")
        return unit_intent == LockIntent.RETRY_HOLD.value and not self.is_granted()

    def get_current_operation(self) -> Operation | None:
        """Return the head operation for this unit, if any."""
        return self._operations.peek()

    def _is_max_retry_reached(self) -> bool:
        """Return True if the head operation exceeded its max_retry (unless max_retry < 0)."""
        operation = self.get_current_operation()
        if not operation:
            return True
        return operation.is_max_retry_reached()

    def _increase_attempt(self) -> None:
        """Increment the attempt counter for the head operation and persist it."""
        raw = self._unit_data.get("operations", "")
        q = OperationQueue.from_string(raw)

        q.increase_attempt()

        self._unit_data.update({"operations": q.to_string()})

    def get_last_completed(self) -> datetime | None:
        """Get the time the unit requested a retry of the head operation."""
        timestamp_str = self._unit_data.get("executed_at", "")
        if timestamp_str:
            return _parse_timestamp(timestamp_str)
        return None

    def get_requested_at(self) -> datetime | None:
        """Get the time the head operation was requested at."""
        operation = self.get_current_operation()
        if not operation:
            return None
        return operation.requested_at

    def _unit_executed_after_grant(self) -> bool:
        granted_at = _parse_timestamp(self._app_data.get("granted_at", ""))
        executed_at = _parse_timestamp(self._unit_data.get("executed_at", ""))

        if granted_at is None or executed_at is None:
            return False
        return executed_at > granted_at


class Locks:
    """Iterator over Lock objects for each unit present on the peer relation."""

    def __init__(self, manager):
        relation = manager.model.relations[manager.relation_name][0]
        units = list(relation.units)
        units.append(manager.model.unit)
        self._units = units
        self._manager = manager

    def __iter__(self):
        """Yields a lock for each unit we can find on the relation."""
        for unit in self._units:
            yield Lock(self._manager, unit=unit)


def pick_oldest_completed(locks: list[Lock]) -> Optional[Lock]:
    """Choose the retry lock with the oldest executed_at timestamp."""
    selected = None
    oldest_timestamp = None

    for lock in locks:
        timestamp = lock.get_last_completed()
        if not timestamp:
            continue

        if oldest_timestamp is None or timestamp < oldest_timestamp:
            oldest_timestamp = timestamp
            selected = lock

    return selected


def pick_oldest_request(locks: list[Lock]) -> Optional[Lock]:
    """Choose the lock with the oldest head operation."""
    selected = None
    oldest_request = None

    for lock in locks:
        timestamp = lock.get_requested_at()
        if not timestamp:
            continue

        if oldest_request is None or timestamp < oldest_request:
            oldest_request = timestamp
            selected = lock

    return selected


class RollingOpsLockGrantedEvent(EventBase):
    """Custom event emitted when the background worker grants the lock."""


class RollingOpsManagerV1(Object):
    """Emitters and handlers for rolling ops."""

    def __init__(self, charm: CharmBase, relation_name: str, callback_targets: dict[str, Any]):
        """Register our custom events.

        params:
            charm: the charm we are attaching this to.
            relation_name: the peer relation name from metadata.yaml.
            callback_targets: mapping from callback_id -> callable.
        """
        super().__init__(charm, "rolling-ops-manager")
        self._charm = charm
        self.relation_name = relation_name
        self.callback_targets = callback_targets
        self.charm_dir = charm.charm_dir
        self.worker = RollingOpsAsyncWorker(charm, relation_name=relation_name)

        charm.on.define_event("rollingop_lock_granted", RollingOpsLockGrantedEvent)

        self.framework.observe(
            charm.on[self.relation_name].relation_changed, self._on_relation_changed
        )
        self.framework.observe(
            charm.on[self.relation_name].relation_departed, self._on_relation_departed
        )
        self.framework.observe(charm.on.leader_elected, self._process_locks)
        self.framework.observe(charm.on.rollingop_lock_granted, self._on_rollingop_granted)
        self.framework.observe(charm.on.update_status, self._on_rollingop_granted)

    @property
    def _relation(self) -> Relation | None:
        return self.model.get_relation(self.relation_name)

    def _on_rollingop_granted(self, event: RollingOpsLockGrantedEvent) -> None:
        if not self._relation:
            return
        logger.info("Received a rolling-op lock granted event.")
        lock = Lock(self)
        if lock.should_run():
            self._on_run_with_lock()

    def _on_relation_departed(self, event: RelationDepartedEvent) -> None:
        """Leader cleanup: if a departing unit was granted, clear the grant.

        This prevents deadlocks when the granted unit leaves the relation.
        """
        if not self.model.unit.is_leader():
            return
        if unit := event.departing_unit:
            lock = Lock(self, unit)
            if lock.is_granted():
                lock.release()
                self._process_locks()

    def _on_relation_changed(self, _: RelationChangedEvent) -> None:
        """Process relation changed."""
        if self.model.unit.is_leader():
            self._process_locks()
            return

        lock = Lock(self)
        if lock.should_run():
            self._on_run_with_lock()

    def _valid_peer_unit_names(self) -> set[str]:
        """Return all unit names currently participating in the peer relation."""
        if not self._relation:
            return set()
        names = {u.name for u in self._relation.units}
        names.add(self.model.unit.name)
        return names

    def _release_stale_grant(self):
        """Ensure granted_unit refers to a unit currently on the peer relation."""
        if not self._relation:
            return

        granted_unit = self._relation.data[self.model.app].get("granted_unit", "")
        if not granted_unit:
            return

        valid_units = self._valid_peer_unit_names()
        if granted_unit not in valid_units:
            logger.warning(
                "granted_unit=%s is not in current peer units; releasing stale grant.",
                granted_unit,
            )
            self._relation.data[self.model.app].update({"granted_unit": "", "granted_at": ""})

    def _process_locks(self, _: EventBase = None):
        """Process locks."""
        if not self.model.unit.is_leader():
            return

        for lock in Locks(self):
            if lock.should_release():
                lock.release()
                break

        self._release_stale_grant()
        granted_unit = self._relation.data[self.model.app].get("granted_unit", "")

        if granted_unit:
            logger.info("Current granted_unit=%s. No new unit will be scheduled.", granted_unit)
            return

        self._schedule()

    def _schedule(self) -> None:
        logger.info("Starting scheduling.")

        pending_requests = []
        pending_retries = []

        for lock in Locks(self):
            if lock.is_retry_hold():
                self._grant_lock(lock)
                return
            if lock.is_waiting():
                pending_requests.append(lock)

            elif lock.is_waiting_retry():
                pending_retries.append(lock)

        selected = None
        if pending_requests:
            selected = pick_oldest_request(pending_requests)
        elif pending_retries:
            selected = pick_oldest_completed(pending_retries)

        if not selected:
            logger.info("No pending lock requests. Lock was not granted to any unit.")
            return

        self._grant_lock(selected)

    def _grant_lock(self, selected: Lock):
        selected.grant()
        logger.info("Lock granted to unit=%s.", selected.unit.name)
        if selected.unit == self.model.unit:
            if selected.is_retry():
                self.worker.start()
                return
            self._on_run_with_lock()

    def request_async_lock(
        self,
        callback_id: str,
        kwargs: dict[str, Any] | None = None,
        max_retry: int | None = None,
    ) -> None:
        """Enqueue a rolling operation and request the distributed lock.

        This method appends an operation (identified by callback_id and kwargs) to the
        calling unit's FIFO queue stored in the peer relation databag and marks the unit as
        requesting the lock. It does not execute the operation directly.

        Args:
            callback_id: Identifier for the callback to execute when this unit is granted
                the lock. Must be a non-empty string and should exist in the manager's
                callback registry.
            kwargs: Keyword arguments to pass to the callback when executed. If omitted,
                an empty dict is used. Must be JSON-serializable because it is stored
                in Juju relation databags.
            max_retry: Retry limit for this operation. None means unlimited retries.
                0 means no retries (drop immediately on first failure). Must be >= 0
                when provided.

        Raises:
            ValueError: If any input is invalid (e.g. empty callback_id, non-dict kwargs,
                non-serializable kwargs, negative max_retry).
            LockNoRelationError: If the peer relation does not exist.
        """
        if callback_id not in self.callback_targets:
            raise ValueError(f"Unknown callback_id: {callback_id}")

        try:
            lock = Lock(self)
            lock.request(callback_id, kwargs, max_retry)

            if self.model.unit.is_leader():
                self._process_locks()
        except (ValueError, TypeError) as e:
            logger.error("Failed to create the lock request: {}".format(e))
            raise e
        except LockNoRelationError as e:
            logger.debug(
                "No {} peer relation yet. Delaying rolling op.".format(self.relation_name)
            )
            raise e

    def _on_run_with_lock(self):
        """Execute the current head operation if this unit holds the distributed lock.

        - If this unit does not currently hold the lock grant, no operation is run.
        - If this unit holds the grant but has no queued operation, lock is released.
        - Otherwise, the operation's callback is looked up by `callback_id` and
            invoked with the operation kwargs.
        """
        lock = Lock(self)
        try:
            if not lock.is_granted():
                logger.debug("Lock is not granted. Operation will not run.")
                return
            operation = lock.get_current_operation()
            if not operation:
                logger.debug("There is no operation to run.")
                lock.complete()
                return

            callback = self.callback_targets.get(operation.callback_id, "")
            logger.debug(
                "Executing callback_id=%s,  attempt=%s", operation.callback_id, operation.attempt
            )

            try:
                result = callback(**operation.kwargs)
            except Exception as e:
                logger.error("Operation failed: %s: %s", operation.callback_id, e)
                result = OperationResult.RETRY_RELEASE

            if result == OperationResult.RETRY_HOLD:
                logger.info(
                    "Finished %s. Operation will be retried immediately.", operation.callback_id
                )
                lock.retry_hold()

            elif result == OperationResult.RETRY_RELEASE:
                logger.info("Finished %s. Operation will be retried later.", operation.callback_id)
                lock.retry_release()
            else:
                logger.info("Finished %s. Lock will be released.", operation.callback_id)
                lock.complete()
        finally:
            if self.model.unit.is_leader():
                self._process_locks()


class RollingOpsAsyncWorker(Object):
    """Spawns and manages the external rolling-ops worker process."""

    def __init__(self, charm: CharmBase, relation_name: str):
        super().__init__(charm, "rollingops-async-worker")
        self._charm = charm
        self._peers_name = relation_name
        self._run_cmd = (
            "/usr/bin/juju-exec" if self.model.juju_version.major > 2 else "/usr/bin/juju-run"
        )

    @property
    def _relation(self):
        return self._charm.model.get_relation(self._peers_name)

    @property
    def _app_data(self):
        return self._relation.data[self.model.app]

    def start(self):
        """Start a new worker process."""
        if self._relation is None:
            return
        self.stop()

        # Remove JUJU_CONTEXT_ID so juju-run works from the spawned process
        new_env = os.environ.copy()
        new_env.pop("JUJU_CONTEXT_ID", None)

        for loc in new_env.get("PYTHONPATH", "").split(":"):
            path = Path(loc)
            venv_path = (
                path
                / ".."
                / "venv"
                / "lib"
                / f"python{version_info.major}.{version_info.minor}"
                / "site-packages"
            )
            if path.stem == "lib":
                new_env["PYTHONPATH"] = f"{venv_path.resolve()}:{new_env['PYTHONPATH']}"
                break

        worker = self._charm.charm_dir / "lib/charms/rolling_ops/v1" / "rollingops.py"

        pid = subprocess.Popen(
            [
                "/usr/bin/python3",
                "-u",
                str(worker),
                "--run-cmd",
                self._run_cmd,
                "--unit-name",
                self._charm.model.unit.name,
                "--charm-dir",
                str(self._charm.charm_dir),
            ],
            cwd=str(self._charm.charm_dir),
            stdout=open("/var/log/rollingops_worker.log", "a"),
            stderr=subprocess.STDOUT,
            env=new_env,
        ).pid

        self._app_data.update({"rollingops-worker-pid": str(pid)})
        logger.info("Started RollingOps worker process with PID %s", pid)

    def stop(self):
        """Stop the running worker process if it exists."""
        if self._relation is None:
            return
        pid_str = self._app_data.get("rollingops-worker-pid", "")
        if not pid_str:
            return

        pid = int(pid_str)
        try:
            os.kill(pid, signal.SIGINT)
            logger.info("Stopped RollingOps worker process PID %s", pid)
        except OSError:
            pass
        self._app_data.update({"rollingops-worker-pid": ""})


def main():
    """Juju hook event dispatcher."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-cmd", required=True)
    parser.add_argument("--unit-name", required=True)
    parser.add_argument("--charm-dir", required=True)
    args = parser.parse_args()

    time.sleep(10)
    dispatch_sub_cmd = f"JUJU_DISPATCH_PATH=hooks/rollingop_lock_granted {args.charm_dir}/dispatch"
    res = subprocess.run([args.run_cmd, "-u", args.unit_name, dispatch_sub_cmd])
    res.check_returncode()


if __name__ == "__main__":
    main()
