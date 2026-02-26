# Copyright 2022 Canonical Ltd.
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
#
# Learn more about testing at: https://juju.is/docs/sdk/testing

import json
from datetime import datetime, timezone

import pytest
from charms.rolling_ops.v1.rollingops import (
    TIMESTAMP_FORMAT,
    Operation,
    OperationQueue,
    _now_timestamp,
    _now_timestamp_str,
)
from ops.testing import Context, PeerRelation, State

from charm import CharmRollingOpsCharm


def _decode_queue_string(queue_str: str) -> list[dict]:
    """Helper: decode OperationQueue.to_string() -> list of dicts."""
    items = json.loads(queue_str)
    assert isinstance(items, list)
    return [json.loads(s) for s in items]


def _unit_databag(state: State, peer: PeerRelation) -> dict:
    rel = state.get_relation(peer.id)
    return rel.local_unit_data


def _app_databag(state: State, peer: PeerRelation) -> dict:
    rel = state.get_relation(peer.id)
    return rel.local_app_data


def _make_operation_queue(callback_id: str, kwargs: dict, max_retry: int | None) -> OperationQueue:
    q = OperationQueue()
    q.enqueue_lock_request(callback_id=callback_id, kwargs=kwargs, max_retry=max_retry)
    return q


def test_operation_create_sets_fields():
    op = Operation.create("restart", {"b": 2, "a": 1}, max_retry=3)

    assert op.kwargs == {"b": 2, "a": 1}
    assert op.callback_id == "restart"
    assert op.max_retry == 3
    assert isinstance(op.requested_at, datetime)


def test_operation_to_string_contains_string_values_only():
    ts = datetime(2026, 2, 23, 12, 0, 0, 123456, tzinfo=timezone.utc)
    op = Operation(
        callback_id="cb", kwargs={"b": 2, "a": 1}, requested_at=ts, max_retry=None, attempt=0
    )

    s = op.to_string()
    obj = json.loads(s)

    assert obj["callback_id"] == "cb"
    assert obj["kwargs"] == '{"a":1,"b":2}'
    assert obj["requested_at"] == ts.strftime(TIMESTAMP_FORMAT)
    assert obj.get("max_retry", "") == ""


def test_operation_equality_and_hash_ignore_timestamp_and_max_retry():
    # Equality only depends on (callback_id, kwargs)
    op1 = Operation.create("restart", {"a": 1, "b": 2}, max_retry=0)
    op2 = Operation.create("restart", {"b": 2, "a": 1}, max_retry=999)

    assert op1 == op2
    assert hash(op1) == hash(op2)

    op3 = Operation.create("restart", {"a": 2}, max_retry=0)
    assert op1 != op3


def test_operation_to_string_and_from_string():
    ts = datetime(2026, 2, 23, 12, 0, 0, 0, tzinfo=timezone.utc)
    op1 = Operation(
        callback_id="cb", kwargs={"x": 1, "y": "z"}, requested_at=ts, max_retry=5, attempt=0
    )

    s = op1.to_string()
    op2 = Operation.from_string(s)

    assert op2.callback_id == op1.callback_id
    assert op2.kwargs == op1.kwargs
    assert op2.requested_at == op1.requested_at
    assert op2.max_retry == op1.max_retry


def test_queue_empty_behaviour():
    q = OperationQueue()

    assert len(q) == 0
    assert q.is_empty() is True
    assert q.peek() is None
    assert q.dequeue() is None

    assert json.loads(q.to_string()) == []


def test_queue_enqueue_and_fifo_order():
    q = OperationQueue()
    assert q.enqueue_lock_request("a", {"i": 1}) is True
    assert q.enqueue_lock_request("b", {"i": 2}) is True

    assert len(q) == 2
    assert q.peek().callback_id == "a"

    first = q.dequeue()
    assert first.callback_id == "a"
    assert len(q) == 1
    assert q.peek().callback_id == "b"

    second = q.dequeue()
    assert second.callback_id == "b"
    assert q.is_empty() is True


def test_queue_deduplicates_only_against_last_item():
    q = OperationQueue()

    assert q.enqueue_lock_request("restart", {"x": 1}) is True
    assert len(q) == 1

    assert q.enqueue_lock_request("restart", {"x": 1}) is False
    assert len(q) == 1

    assert q.enqueue_lock_request("restart", {"x": 2}) is True
    assert len(q) == 2

    assert q.enqueue_lock_request("restart", {"x": 1}) is True
    assert len(q) == 3


def test_queue_to_string_and_from_string():
    q1 = OperationQueue()
    q1.enqueue_lock_request("a", {"x": 1}, max_retry=5)
    q1.enqueue_lock_request("b", {"y": "z"}, max_retry=None)

    encoded = q1.to_string()
    q2 = OperationQueue.from_string(encoded)

    assert len(q2) == 2
    assert q2.peek().callback_id == "a"
    assert q2.dequeue().callback_id == "a"
    assert q2.dequeue().callback_id == "b"
    assert q2.is_empty()


def test_queue_from_string_empty_string_is_empty_queue():
    q = OperationQueue.from_string("")
    assert q.is_empty()
    assert q.peek() is None


def test_queue_from_string_rejects_non_list_json():
    with pytest.raises(ValueError, match="JSON list"):
        OperationQueue.from_string(json.dumps({"not": "a list"}))


def test_queue_encoding_is_list_of_operation_strings():
    q = OperationQueue()
    q.enqueue_lock_request("a", {"x": 1})
    s = q.to_string()

    decoded = json.loads(s)
    assert isinstance(decoded, list)
    assert len(decoded) == 1
    assert isinstance(decoded[0], str)

    op_dicts = _decode_queue_string(s)
    assert op_dicts[0]["callback_id"] == "a"
    assert op_dicts[0]["kwargs"] == '{"x":1}'
    assert op_dicts[0].get("max_retry", "") == ""
    assert "requested_at" in op_dicts[0]


def test_lock_request_enqueues_and_sets_request():
    ctx = Context(CharmRollingOpsCharm)
    peer = PeerRelation(endpoint="restart")
    state_in = State(leader=False, relations={peer})

    state_out = ctx.run(
        ctx.on.action("restart", params={"delay": 10}),
        state_in,
    )

    databag = _unit_databag(state_out, peer)
    assert databag["state"] == "request"
    assert databag["operations"]

    q = OperationQueue.from_string(databag["operations"])
    len(q) == 1
    operation = q.peek()
    assert operation.callback_id == "_restart"
    assert operation.kwargs == {"delay": 10}
    assert operation.max_retry is None
    assert operation.requested_at is not None


@pytest.mark.parametrize(
    "callback_id, kwargs, max_retry",
    [
        # callback_id
        ("", {}, 0),
        ("   ", {}, 0),
        ("unknown", {}, 0),
        (None, {}, 0),
        (123, {}, 0),
        # kwargs
        ("_restart", "nope", 0),
        ("_restart", [], 0),
        ("_restart", {"x": OperationQueue()}, 0),
        # max_retry
        ("_restart", {}, -5),
        ("_restart", {}, -1),
        ("_restart", {}, "3"),
    ],
)
def test_lock_request_invalid_inputs(callback_id, kwargs, max_retry):
    ctx = Context(CharmRollingOpsCharm)
    peer = PeerRelation(endpoint="restart")
    state_in = State(leader=False, relations={peer})

    with ctx(ctx.on.update_status(), state_in) as mgr:
        with pytest.raises(ValueError):
            mgr.charm.restart_manager.request_async_lock(
                callback_id=callback_id,
                kwargs=kwargs,
                max_retry=max_retry,
            )


def test_existing_operation_then_new_request():
    ctx = Context(CharmRollingOpsCharm)
    queue = _make_operation_queue(callback_id="_failed_restart", kwargs={}, max_retry=3)
    peer = PeerRelation(
        endpoint="restart",
        local_unit_data={"state": "request", "operations": queue.to_string()},
    )

    state_in = State(leader=False, relations={peer})

    state_out = ctx.run(ctx.on.action("restart", params={"delay": 10}), state_in)

    databag = _unit_databag(state_out, peer)
    assert databag["state"] == "request"
    result = OperationQueue.from_string(databag["operations"])

    assert len(result) == 2
    assert result.operations[0].callback_id == "_failed_restart"
    assert result.operations[1].callback_id == "_restart"


def test_new_request_does_not_overwrite_state_if_queue_not_empty():
    ctx = Context(CharmRollingOpsCharm)
    queue = _make_operation_queue(callback_id="_failed_restart", kwargs={}, max_retry=3)
    executed_at = _now_timestamp_str()
    peer = PeerRelation(
        endpoint="restart",
        local_unit_data={
            "state": "retry",
            "executed_at": executed_at,
            "operations": queue.to_string(),
        },
    )
    state_in = State(leader=False, relations={peer})

    state_out = ctx.run(ctx.on.action("restart", params={"delay": 10}), state_in)

    databag = _unit_databag(state_out, peer)
    assert databag["state"] == "retry"
    assert databag["executed_at"] == executed_at
    result = OperationQueue.from_string(databag["operations"])
    assert len(result) == 2
    assert result.operations[0].callback_id == "_failed_restart"
    assert result.operations[1].callback_id == "_restart"


def test_relation_changed_without_grant_does_not_run_operation():
    ctx = Context(CharmRollingOpsCharm)
    remote_unit_name = f"{ctx.app_name}/1"
    queue = _make_operation_queue(callback_id="_failed_restart", kwargs={}, max_retry=3)
    peer = PeerRelation(
        endpoint="restart",
        local_unit_data={"state": "request", "operations": queue.to_string()},
        local_app_data={"granted_unit": remote_unit_name, "granted_at": _now_timestamp_str()},
    )

    state_in = State(leader=False, relations={peer})

    state_out = ctx.run(ctx.on.relation_changed(peer, remote_unit=remote_unit_name), state_in)

    databag = _unit_databag(state_out, peer)
    assert databag["state"] == "request"
    result = OperationQueue.from_string(databag["operations"])
    assert len(result) == 1
    assert databag.get("executed_at", "") == ""


def test_lock_complete_pops_head():
    ctx = Context(CharmRollingOpsCharm)
    remote_unit_name = f"{ctx.app_name}/1"
    local_unit_name = f"{ctx.app_name}/0"
    queue = _make_operation_queue(callback_id="_restart", kwargs={}, max_retry=0)
    peer = PeerRelation(
        endpoint="restart",
        local_unit_data={"state": "request", "operations": queue.to_string()},
        local_app_data={"granted_unit": local_unit_name, "granted_at": _now_timestamp_str()},
    )
    state_in = State(leader=False, relations={peer})

    state_out = ctx.run(ctx.on.relation_changed(peer, remote_unit=remote_unit_name), state_in)

    databag = _unit_databag(state_out, peer)
    assert databag["state"] == "idle"
    assert databag["executed_at"] is not None
    assert databag.get("operations", None) == "[]"

    q = OperationQueue.from_string(databag["operations"])
    assert len(q) == 0


def test_successful_operation_leaves_state_request_when_more_ops_remain():
    ctx = Context(CharmRollingOpsCharm)
    local_unit_name = f"{ctx.app_name}/0"
    remote_unit_name = f"{ctx.app_name}/1"
    queue = OperationQueue()
    queue.enqueue_lock_request(callback_id="_restart", kwargs={}, max_retry=None)
    queue.enqueue_lock_request(callback_id="_failed_restart", kwargs={}, max_retry=None)

    peer = PeerRelation(
        endpoint="restart",
        local_unit_data={"state": "request", "operations": queue.to_string()},
        local_app_data={"granted_unit": local_unit_name, "granted_at": _now_timestamp_str()},
    )

    state_in = State(leader=False, relations={peer})

    state_out = ctx.run(ctx.on.relation_changed(peer, remote_unit=remote_unit_name), state_in)

    databag = _unit_databag(state_out, peer)
    assert databag["state"] == "request"
    q = OperationQueue.from_string(databag["operations"])
    assert len(q) == 1
    current_operation = q.peek()
    assert current_operation.callback_id == "_failed_restart"


def test_lock_retry_marks_retry():
    ctx = Context(CharmRollingOpsCharm)
    remote_unit_name = f"{ctx.app_name}/1"
    local_unit_name = f"{ctx.app_name}/0"
    queue = _make_operation_queue(callback_id="_failed_restart", kwargs={}, max_retry=3)
    peer = PeerRelation(
        endpoint="restart",
        local_unit_data={"state": "request", "operations": queue.to_string()},
        local_app_data={"granted_unit": local_unit_name, "granted_at": _now_timestamp_str()},
    )
    state_in = State(leader=False, relations={peer})

    state_out = ctx.run(ctx.on.relation_changed(peer, remote_unit=remote_unit_name), state_in)

    databag = _unit_databag(state_out, peer)
    assert databag["state"] == "retry"
    assert databag["executed_at"] is not None

    q = OperationQueue.from_string(databag["operations"])
    assert len(q) == 1
    current_operation = q.peek()
    initial_operation = queue.peek()
    assert current_operation.callback_id == initial_operation.callback_id
    assert current_operation.kwargs == initial_operation.kwargs
    assert current_operation.max_retry == initial_operation.max_retry
    assert current_operation.requested_at == initial_operation.requested_at
    assert current_operation.attempt == 1


def test_lock_retry_drops_when_max_retry_reached():
    ctx = Context(CharmRollingOpsCharm)
    remote_unit_name = f"{ctx.app_name}/1"
    local_unit_name = f"{ctx.app_name}/0"
    operation = Operation(
        callback_id="_failed_restart",
        kwargs={},
        max_retry=3,
        requested_at=_now_timestamp(),
        attempt=3,
    )
    queue = OperationQueue()
    queue._enqueue(operation)
    peer = PeerRelation(
        endpoint="restart",
        local_unit_data={"state": "request", "operations": queue.to_string()},
        local_app_data={"granted_unit": local_unit_name, "granted_at": _now_timestamp_str()},
    )
    state_in = State(leader=False, relations={peer})

    state_out = ctx.run(ctx.on.relation_changed(peer, remote_unit=remote_unit_name), state_in)

    databag = _unit_databag(state_out, peer)
    assert databag["state"] == "idle"
    assert databag["executed_at"] is not None

    q = OperationQueue.from_string(databag["operations"])
    assert len(q) == 0


def test_lock_grant_and_release():
    ctx = Context(CharmRollingOpsCharm)
    queue = _make_operation_queue(callback_id="_failed_restart", kwargs={}, max_retry=3)
    peer = PeerRelation(
        endpoint="restart", peers_data={1: {"state": "request", "operations": queue.to_string()}}
    )
    state = State(leader=True, relations={peer})

    state = ctx.run(ctx.on.leader_elected(), state)
    databag = _app_databag(state, peer)

    unit_name = f"{ctx.app_name}/1"
    assert unit_name in databag["granted_unit"]
    assert databag["granted_at"] is not None


def test_scheduling_does_nothing_if_lock_already_granted():
    ctx = Context(CharmRollingOpsCharm)
    queue = _make_operation_queue(callback_id="_failed_restart", kwargs={}, max_retry=3)
    remote_unit_name = f"{ctx.app_name}/1"
    now_timestamp = _now_timestamp_str()
    peer = PeerRelation(
        endpoint="restart",
        peers_data={
            1: {"state": "request", "operations": queue.to_string()},
            2: {"state": "request", "operations": queue.to_string()},
        },
        local_app_data={"granted_unit": remote_unit_name, "granted_at": now_timestamp},
    )
    state_in = State(leader=True, relations={peer})

    state_out = ctx.run(ctx.on.relation_changed(peer, remote_unit=remote_unit_name), state_in)

    databag = _app_databag(state_out, peer)
    assert databag["granted_unit"] == remote_unit_name
    assert databag["granted_at"] == now_timestamp


def test_schedule_picks_oldest_requested_at_among_requests():
    ctx = Context(CharmRollingOpsCharm)

    old_queue = OperationQueue()
    old_operation = Operation(
        callback_id="restart", kwargs={}, max_retry=2, requested_at=_now_timestamp(), attempt=0
    )
    old_queue._enqueue(old_operation)

    new_queue = OperationQueue()
    new_operation = Operation(
        callback_id="restart", kwargs={}, max_retry=2, requested_at=_now_timestamp(), attempt=0
    )
    new_queue._enqueue(new_operation)

    peer = PeerRelation(
        endpoint="restart",
        peers_data={
            1: {"state": "request", "operations": new_queue.to_string()},
            2: {"state": "request", "operations": old_queue.to_string()},
        },
    )
    state_in = State(leader=True, relations={peer})

    state_out = ctx.run(ctx.on.leader_elected(), state_in)
    databag = _app_databag(state_out, peer)
    remote_unit_name = f"{ctx.app_name}/2"
    assert databag["granted_unit"] == remote_unit_name


def test_schedule_picks_oldest_executed_at_among_retries_when_no_requests():
    ctx = Context(CharmRollingOpsCharm)

    old_operation = _now_timestamp_str()
    queue = _make_operation_queue(callback_id="_failed_restart", kwargs={}, max_retry=3)
    new_operation = _now_timestamp_str()

    peer = PeerRelation(
        endpoint="restart",
        peers_data={
            1: {
                "state": "retry",
                "operations": queue.to_string(),
                "executed_at": new_operation,
            },
            2: {
                "state": "retry",
                "operations": queue.to_string(),
                "executed_at": old_operation,
            },
        },
    )
    state_in = State(leader=True, relations={peer})

    state_out = ctx.run(ctx.on.leader_elected(), state_in)

    databag = _app_databag(state_out, peer)
    remote_unit_name = f"{ctx.app_name}/2"
    assert databag["granted_unit"] == remote_unit_name


def test_schedule_prioritizes_requests_over_retries():
    ctx = Context(CharmRollingOpsCharm)
    queue = _make_operation_queue(callback_id="_failed_restart", kwargs={}, max_retry=3)

    peer = PeerRelation(
        endpoint="restart",
        peers_data={
            1: {
                "state": "retry",
                "operations": queue.to_string(),
                "executed_at": _now_timestamp_str(),
            },
            2: {"state": "request", "operations": queue.to_string()},
        },
    )
    state_in = State(leader=True, relations={peer})

    state_out = ctx.run(ctx.on.leader_elected(), state_in)

    databag = _app_databag(state_out, peer)
    remote_unit_name = f"{ctx.app_name}/2"
    assert databag["granted_unit"] == remote_unit_name


def test_no_unit_is_granted_if_there_are_no_requests():
    ctx = Context(CharmRollingOpsCharm)
    peer = PeerRelation(
        endpoint="restart", peers_data={1: {"state": "idle"}, 2: {"state": "idle"}}
    )
    state_in = State(leader=True, relations={peer})

    state_out = ctx.run(ctx.on.leader_elected(), state_in)

    databag = _app_databag(state_out, peer)
    assert databag.get("granted_unit", "") == ""
    assert databag.get("granted_at", "") == ""
