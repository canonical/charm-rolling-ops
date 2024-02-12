# Copyright 2024 Canonical Ltd.
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

import asyncio
import json
import logging
import time
from types import SimpleNamespace

import pytest
from juju.action import Action
from juju.application import Application
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)

MODEL_CONFIG = {
    "logging-config": "<root>=DEBUG;unit=DEBUG",
    "update-status-hook-interval": "5m",
}

TARGET_SERIES = ["focal"]  # , "jammy"]
NUM_UNITS = 3
UPDATE_STATUS_IN_SECONDS = 60


TestTypeNamespace = SimpleNamespace(
    restart_1_unit="restart_1_unit",
    restart_all_units="restart_all_units",
    remove_unit_and_restart="remove_unit_and_restart",
    remove_leader_and_restart="remove_leader_and_restart",
    stop_unit_container="stop_unit_container",
)


DEPLOY_ALL_GROUP_MARKS = [
    (
        pytest.param(
            series, test, id=f"{series}_{test}", marks=pytest.mark.group(f"{series}_{test}")
        )
    )
    for series in TARGET_SERIES
    for test in [
        TestTypeNamespace.restart_1_unit,
        TestTypeNamespace.restart_all_units,
        TestTypeNamespace.remove_unit_and_restart,
        TestTypeNamespace.remove_leader_and_restart,
        TestTypeNamespace.stop_unit_container,
    ]
]


async def get_leader_unit_id(ops_test: OpsTest, app: Application) -> int:
    """Helper function that retrieves the leader unit ID."""
    leader_unit = None
    for unit in ops_test.model.applications[app.charm_name].units:
        if await unit.is_leader_from_status():
            leader_unit = unit
            break

    return int(leader_unit.name.split("/")[1])


async def get_a_non_leader_unit_id(ops_test: OpsTest, app: Application) -> int:
    """Helper function that retrieves the leader unit ID."""
    leader_id = await get_leader_unit_id(ops_test, app)
    for id in range(NUM_UNITS):
        if id != leader_id:
            return id
    raise Exception()


async def run_action_on_leader(ops_test: OpsTest, app: Application, action_name: str) -> None:
    """Helper function that runs an action on the leader unit."""
    leader_unit_id = await get_leader_unit_id(ops_test, app)
    return await app.units[leader_unit_id].run_action(action_name)


async def _get_unit_lock_status(ops_test: OpsTest, app: Application):
    """Helper function that retrieves the lock status of a unit."""
    action: Action = await run_action_on_leader(ops_test, app, "get-lock-status")
    action = await action.wait()
    assert action.status == "completed"
    return action.results


async def assert_restart_1_unit(ops_test: OpsTest, app, unit_id: int) -> None:
    # Trigger some chatting in the relation changed
    for i in range(10):
        await app.units[i % NUM_UNITS].run_action("share-useless-data")

    await ops_test.model.block_until(lambda: app.status in ("active"))

    # Ensure there is no lock granted
    # original_status = await _get_unit_lock_status(ops_test, app)
    original_ts = float(time.time())

    # Run the restart action
    action: Action = await app.units[unit_id].run_action("restart")
    action = await action.wait()
    assert action.status == "completed"

    # Confirm the action lock has been requested
    current_status = await _get_unit_lock_status(ops_test, app)
    timestamps = json.loads(current_status["timestamps"].replace("'", '"'))
    requested_ts = float(timestamps[f"rolling-ops/{unit_id}"]["requested"])
    assert requested_ts > original_ts
    # Check if other units did not request it after original_ts:
    for i in range(NUM_UNITS):
        if i != unit_id:
            assert float(timestamps[f"rolling-ops/{i}"]["requested"]) < original_ts

    # Wait for a few update status cycles
    await asyncio.sleep(2 * UPDATE_STATUS_IN_SECONDS)
    current_status: Action = await run_action_on_leader(ops_test, app, "get-lock-status")
    current_status = await current_status.wait()
    assert current_status.status == "completed"

    # Confirm the lock has been released:
    current_status = await _get_unit_lock_status(ops_test, app)
    timestamps = json.loads(current_status["timestamps"].replace("'", '"'))
    released_ts = float(timestamps[f"rolling-ops/{unit_id}"]["released"])
    assert released_ts > requested_ts
    # Check if other units did not request it after original_ts:
    for i in range(NUM_UNITS):
        if i != unit_id:
            assert float(timestamps[f"rolling-ops/{i}"]["requested"]) < original_ts
