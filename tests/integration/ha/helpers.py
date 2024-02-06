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
import logging
import time

import pytest
from juju.action import Action
from juju.application import Application
from juju.model import Model
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)

MODEL_CONFIG = {
    "logging-config": "<root>=DEBUG;unit=DEBUG",
    "update-status-hook-interval": "5m",
}
GROUP_MARKS = [
    "restart-1-unit",
    "restart-all-units-at-once",
    "remove-unit-and-restart",
    "remove-leader-and-restart" "stop-unit-container",
]
TARGET_SERIES = ["focal", "jammy"]
NUM_UNITS = 3
UPDATE_STATUS_IN_SECONDS = 60


@pytest.fixture
async def model(ops_test: OpsTest) -> Model:
    return ops_test.model


@pytest.fixture
async def app(model) -> Application:
    return model.applications["rolling-ops"]


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


async def _assert_restart_1_unit(ops_test: OpsTest, app, unit_id: int) -> None:
    # Trigger some chatting in the relation changed
    for i in range(10):
        await app.units[i % NUM_UNITS].run_action("share-useless-data")

    # Ensure there is no lock granted
    original_status: Action = await run_action_on_leader("get-lock-status")
    original_ts = time.time()
    assert original_status.status == "completed"

    # Run the restart action
    action: Action = await app.units[unit_id].run_action("restart", delay="0")
    # Confirm the action lock has been requested
    assert action.status == "completed"
    current_status: Action = await run_action_on_leader("get-lock-status")
    assert current_status.status == "completed"
    assert (
        current_status.results["timestamps"][f"rolling-ops/{unit_id}"]["requested"] > original_ts
    )
    requested_ts = current_status.results["timestamps"][f"rolling-ops/{unit_id}"]["requested"]

    # Wait for a few update status cycles
    await asyncio.sleep(2 * UPDATE_STATUS_IN_SECONDS)
    current_status: Action = await run_action_on_leader("get-lock-status")
    assert current_status.status == "completed"
    assert (
        current_status.results["timestamps"][f"rolling-ops/{unit_id}"]["requested"] > original_ts
    )
    # Lock not requested again
    assert (
        current_status.results["timestamps"][f"rolling-ops/{unit_id}"]["requested"] == requested_ts
    )
    # Ensure the restart has been answered
    assert (
        current_status.results["timestamps"][f"rolling-ops/{unit_id}"]["released"] > requested_ts
    )
    released_ts = current_status.results["timestamps"][f"rolling-ops/{unit_id}"]["released"]

    # Trigger some chatting in the relation changed
    for i in range(10):
        await app.units[i % NUM_UNITS].run_action("share-useless-data")
    await asyncio.sleep(2 * UPDATE_STATUS_IN_SECONDS)
    current_status: Action = await run_action_on_leader("get-lock-status")
    assert current_status.status == "completed"
    assert (
        current_status.results["timestamps"][f"rolling-ops/{unit_id}"]["requested"] > original_ts
    )
    # Lock not requested again
    assert (
        current_status.results["timestamps"][f"rolling-ops/{unit_id}"]["requested"] == requested_ts
    )
    # Lock not released again
    assert (
        current_status.results["timestamps"][f"rolling-ops/{unit_id}"]["released"] == released_ts
    )
