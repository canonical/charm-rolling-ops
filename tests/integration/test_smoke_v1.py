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
#
# Learn more about testing at: https://juju.is/docs/sdk/testing

import json
import logging
import shutil
import time
from datetime import datetime

import pytest
from juju.unit import Unit
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


TRACE_FILE = "/var/lib/charm-rolling-ops/transitions.log"


@pytest.fixture(scope="module", autouse=True)
def copy_rolling_ops_library_into_charm(ops_test: OpsTest):
    """Copy the data_interfaces library to the different charm folder."""
    library_path = "lib/charms/rolling_ops/v1/rollingops.py"
    install_path = "tests/charms/v1/" + library_path
    shutil.copyfile(library_path, install_path)


async def get_unit_events(unit: Unit) -> list[dict]:
    action = await unit.run(f"cat {TRACE_FILE}")
    await action.wait()

    stdout = action.results.get("stdout", "")
    if not stdout.strip():
        return []

    return [json.loads(line) for line in stdout.strip().splitlines()]


def parse_ts(event: dict) -> datetime:
    return datetime.fromisoformat(event["ts"])


@pytest.mark.abort_on_fail
async def test_restart_action_one_unit(ops_test: OpsTest):
    """Verify that restart action runs through the expected workflow."""
    charm = await ops_test.build_charm("tests/charms/v1")

    await ops_test.model.deploy(charm, application_name="rollingops", num_units=1)
    await ops_test.model.wait_for_idle(status="active")

    unit = ops_test.model.applications["rollingops"].units[0]

    action = await unit.run_action("restart", delay=1)
    await action.wait()
    await ops_test.model.wait_for_idle(status="active", timeout=300)

    events = await get_unit_events(unit)
    restart_events = [e["event"] for e in events]

    expected = [
        "action:restart",
        "_restart:start",
        "_restart:done",
    ]

    assert restart_events == expected, f"unexpected event order: {restart_events}"


@pytest.mark.abort_on_fail
async def test_failed_restart_retries_one_unit(ops_test):

    unit = ops_test.model.applications["rollingops"].units[0]
    rm = await unit.run(f"rm -f {TRACE_FILE}")
    await rm.wait()
    action = await unit.run_action("failed-restart", **{"delay": 1, "max-retry": 2})
    await action.wait()

    await ops_test.model.wait_for_idle()

    events = await get_unit_events(unit)
    restart_events = [e["event"] for e in events]

    expected = [
        "action:failed-restart",
        "_failed_restart:start",  # attempt 0
        "_failed_restart:retry_release",
        "_failed_restart:start",  # retry 1
        "_failed_restart:retry_release",
        "_failed_restart:start",  # retry 2
        "_failed_restart:retry_release",
    ]

    assert restart_events == expected, f"unexpected event order: {restart_events}"


@pytest.mark.abort_on_fail
async def test_deferred_restart_retries_one_unit(ops_test):

    unit = ops_test.model.applications["rollingops"].units[0]
    rm = await unit.run(f"rm -f {TRACE_FILE}")
    await rm.wait()
    action = await unit.run_action("deferred-restart", **{"delay": 1, "max-retry": 2})
    await action.wait()

    await ops_test.model.wait_for_idle()

    events = await get_unit_events(unit)
    restart_events = [e["event"] for e in events]

    expected = [
        "action:deferred-restart",
        "_deferred_restart:start",  # attempt 0
        "_deferred_restart:retry_hold",
        "_deferred_restart:start",  # retry 1
        "_deferred_restart:retry_hold",
        "_deferred_restart:start",  # retry 2
        "_deferred_restart:retry_hold",
    ]

    assert restart_events == expected, f"unexpected event order: {restart_events}"


@pytest.mark.abort_on_fail
async def test_restart_rolls_one_unit_at_a_time(ops_test):

    await ops_test.model.applications["rollingops"].add_unit(count=4)
    await ops_test.model.wait_for_idle(wait_for_at_least_units=5)

    units = ops_test.model.applications["rollingops"].units

    for unit in units:
        rm = await unit.run(f"rm -f {TRACE_FILE}")
        await rm.wait()

    for unit in units:
        await unit.run_action("restart", **{"delay": 2})

    await ops_test.model.wait_for_idle(status="active", timeout=600)

    all_events = []
    for unit in units:
        events = await get_unit_events(unit)
        assert len(events) == 3
        all_events.extend(events)

    restart_events = [e for e in all_events if e["event"] in {"_restart:start", "_restart:done"}]
    restart_events.sort(key=parse_ts)

    logger.info(restart_events)

    for i in range(0, len(restart_events), 2):
        start_event = restart_events[i]
        done_event = restart_events[i + 1]

        assert start_event["event"] == "_restart:start"
        assert done_event["event"] == "_restart:done"
        assert start_event["unit"] == done_event["unit"], (
            f"start/done pair mismatch: {start_event} vs {done_event}"
        )


@pytest.mark.abort_on_fail
async def test_retry_hold_keeps_lock_on_same_unit(ops_test):
    units = ops_test.model.applications["rollingops"].units
    for unit in units:
        rm = await unit.run(f"rm -f {TRACE_FILE}")
        await rm.wait()

    unit_a = units[1]
    unit_b = units[3]

    action = await unit_a.run_action("deferred-restart", **{"delay": 10, "max-retry": 2})
    await action.wait()
    await unit_b.run_action("restart", **{"delay": 2})

    await ops_test.model.wait_for_idle()

    all_events = []
    all_events.extend(await get_unit_events(unit_a))
    all_events.extend(await get_unit_events(unit_b))
    all_events.sort(key=parse_ts)

    logger.info(all_events)

    relevant_events = [
        e
        for e in all_events
        if e["event"]
        in {
            "_deferred_restart:start",
            "_deferred_restart:retry_hold",
            "_restart:start",
            "_restart:done",
        }
    ]

    sequence = [(e["unit"], e["event"]) for e in relevant_events]

    logger.info(sequence)

    assert sequence == [
        (unit_a.name, "_deferred_restart:start"),  # attempt 0
        (unit_a.name, "_deferred_restart:retry_hold"),
        (unit_a.name, "_deferred_restart:start"),  # retry 1
        (unit_a.name, "_deferred_restart:retry_hold"),
        (unit_a.name, "_deferred_restart:start"),  # retry 2
        (unit_a.name, "_deferred_restart:retry_hold"),
        (unit_b.name, "_restart:start"),
        (unit_b.name, "_restart:done"),
    ], f"unexpected event sequence: {sequence}"


@pytest.mark.abort_on_fail
async def test_retry_release_alternates_execution(ops_test):
    units = ops_test.model.applications["rollingops"].units
    for unit in units:
        rm = await unit.run(f"rm -f {TRACE_FILE}")
        await rm.wait()

    unit_a = units[2]
    unit_b = units[4]

    action = await unit_a.run_action("failed-restart", **{"delay": 10, "max-retry": 2})
    await action.wait()
    await unit_b.run_action("failed-restart", **{"delay": 1, "max-retry": 2})

    await ops_test.model.wait_for_idle()

    all_events = []
    all_events.extend(await get_unit_events(unit_a))
    all_events.extend(await get_unit_events(unit_b))
    all_events.sort(key=parse_ts)

    logger.info(all_events)

    relevant_events = [
        e
        for e in all_events
        if e["event"] in {"_failed_restart:start", "_failed_restart:retry_release"}
    ]

    sequence = [(e["unit"], e["event"]) for e in relevant_events]

    logger.info(sequence)

    assert sequence == [
        (unit_a.name, "_failed_restart:start"),  # attempt 0
        (unit_a.name, "_failed_restart:retry_release"),
        (unit_b.name, "_failed_restart:start"),  # attempt 0
        (unit_b.name, "_failed_restart:retry_release"),
        (unit_a.name, "_failed_restart:start"),  # retry 1
        (unit_a.name, "_failed_restart:retry_release"),
        (unit_b.name, "_failed_restart:start"),  # retry 1
        (unit_b.name, "_failed_restart:retry_release"),
        (unit_a.name, "_failed_restart:start"),  # retry 2
        (unit_a.name, "_failed_restart:retry_release"),
        (unit_b.name, "_failed_restart:start"),  # retry 2
        (unit_b.name, "_failed_restart:retry_release"),
    ], f"unexpected event sequence: {sequence}"


@pytest.mark.abort_on_fail
async def test_subsequent_lock_request_of_different_ops(ops_test):
    units = ops_test.model.applications["rollingops"].units
    for unit in units:
        rm = await unit.run(f"rm -f {TRACE_FILE}")
        await rm.wait()

    unit_a = units[3]
    unit_b = units[4]

    action = await unit_b.run_action("deferred-restart", **{"delay": 10, "max-retry": 2})
    await action.wait()
    action = await unit_a.run_action("failed-restart", **{"delay": 1, "max-retry": 2})
    await action.wait()
    action = await unit_a.run_action("restart", **{"delay": 1})
    await action.wait()
    action = await unit_a.run_action("deferred-restart", **{"delay": 1, "max-retry": 0})
    await action.wait()
    action = await unit_a.run_action("restart", **{"delay": 1})
    await action.wait()

    await ops_test.model.wait_for_idle()

    unit_a_events = await get_unit_events(unit_a)
    relevant_events = [e["event"] for e in unit_a_events]

    logger.info(relevant_events)

    assert relevant_events == [
        "action:failed-restart",
        "action:restart",
        "action:deferred-restart",
        "action:restart",
        "_failed_restart:start",  # attempt 0
        "_failed_restart:retry_release",
        "_failed_restart:start",  # retry 1
        "_failed_restart:retry_release",
        "_failed_restart:start",  # retry 2
        "_failed_restart:retry_release",
        "_restart:start",
        "_restart:done",
        "_deferred_restart:start",  # attempt 0
        "_deferred_restart:retry_hold",
        "_restart:start",
        "_restart:done",
    ], f"unexpected event sequence: {relevant_events}"


@pytest.mark.abort_on_fail
async def test_subsequent_lock_request_of_same_op(ops_test):
    units = ops_test.model.applications["rollingops"].units
    for unit in units:
        rm = await unit.run(f"rm -f {TRACE_FILE}")
        await rm.wait()

    unit_a = units[3]
    unit_b = units[4]

    action = await unit_b.run_action("deferred-restart", **{"delay": 10, "max-retry": 1})
    await action.wait()
    action = await unit_a.run_action("failed-restart", **{"delay": 1, "max-retry": 2})
    await action.wait()
    for i in range(3):
        action = await unit_a.run_action("restart", **{"delay": 1})
        await action.wait()

    await ops_test.model.wait_for_idle()

    unit_a_events = await get_unit_events(unit_a)
    relevant_events = [e["event"] for e in unit_a_events]

    logger.info(relevant_events)

    assert relevant_events == [
        "action:failed-restart",
        "action:restart",
        "action:restart",
        "action:restart",
        "_failed_restart:start",  # attempt 0
        "_failed_restart:retry_release",
        "_failed_restart:start",  # retry 1
        "_failed_restart:retry_release",
        "_failed_restart:start",  # retry 2
        "_failed_restart:retry_release",
        "_restart:start",
        "_restart:done",
    ], f"unexpected event sequence: {relevant_events}"


@pytest.mark.abort_on_fail
async def test_retry_on_leader_unit_leaves_the_hook(ops_test):
    units = ops_test.model.applications["rollingops"].units
    for unit in units:
        rm = await unit.run(f"rm -f {TRACE_FILE}")
        await rm.wait()

    leader = None
    non_leader = None
    for unit in units:
        if await unit.is_leader_from_status():
            leader = unit
        else:
            non_leader = unit

    action = await leader.run_action("failed-restart", **{"delay": 5})
    await action.wait()
    action = await non_leader.run_action("restart", **{"delay": 3})
    await action.wait()

    await ops_test.model.wait_for_idle(status="active", wait_for_at_least_units=2)

    non_leader_events = await get_unit_events(non_leader)
    relevant_events = [e["event"] for e in non_leader_events]

    assert relevant_events == [
        "action:restart",
        "_restart:start",
        "_restart:done",
    ], f"unexpected event sequence: {relevant_events}"
