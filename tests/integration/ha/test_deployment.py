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
import subprocess
import time

import pytest
from juju.action import Action
from juju.application import Application
from juju.model import Model
from juju.unit import Unit
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


@pytest.mark.parametrize(
    "series",
    [
        (pytest.param("jammy", marks=pytest.mark.group(1)))
        # (pytest.param(series, marks=pytest.mark.group(f"{series}:{GROUP_MARKS[i]}")))
        # for series in TARGET_SERIES
        # for i in range(len(GROUP_MARKS))
    ],
)
@pytest.mark.abort_on_fail
@pytest.mark.skip_if_deployed
async def test_build_and_deploy(ops_test: OpsTest, series, model, app) -> None:
    my_charm = await ops_test.build_charm(".")
    await ops_test.model.set_config(MODEL_CONFIG)
    await ops_test.model.deploy(my_charm, num_units=NUM_UNITS, series=series)
    model: Model = ops_test.model
    app = model.applications["rolling-ops"]

    await ops_test.model.block_until(lambda: app.status in ("error", "blocked", "active"))
    assert app.status == "active"


@pytest.mark.parametrize(
    [
        (pytest.param(series, marks=pytest.mark.group(f"{series}: restart 1 unit")))
        for series in TARGET_SERIES
    ],
)
@pytest.mark.abort_on_fail
async def test_restart_1_unit(ops_test: OpsTest, app) -> None:
    unit_id = await get_a_non_leader_unit_id()
    await _assert_restart_1_unit(ops_test, app, unit_id)


@pytest.mark.parametrize(
    [
        (pytest.param(series, marks=pytest.mark.group(f"{series}: restart all units")))
        for series in TARGET_SERIES
    ],
)
@pytest.mark.abort_on_fail
async def test_restart_all_units(ops_test: OpsTest, app) -> None:
    for i in range(NUM_UNITS):
        await _assert_restart_1_unit(ops_test, app, i)


@pytest.mark.parametrize(
    [
        (
            pytest.param(series, marks=pytest.mark.group(f"{series}: remove one and restart")),
        )
        for series in TARGET_SERIES
    ],
)
@pytest.mark.abort_on_fail
async def test_remove_and_restart(ops_test: OpsTest, app) -> None:
    # Remove a non-leader unit
    unit_id = await get_a_non_leader_unit_id()
    await app.units[unit_id].remove()

    await ops_test.model.block_until(lambda: app.status in ("error", "blocked", "active"))
    assert app.status == "active"
    unit_id = await get_a_non_leader_unit_id()
    await _assert_restart_1_unit(ops_test, app, unit_id)


@pytest.mark.parametrize(
    [
        (
            pytest.param(series, marks=pytest.mark.group(f"{series}: remove leader and restart")),
        )
        for series in TARGET_SERIES
    ],
)
@pytest.mark.abort_on_fail
async def test_remove_leader_and_restart(ops_test: OpsTest, app) -> None:
    # Remove a non-leader unit
    leader_id = await get_leader_unit_id()
    await app.destroy_units(f"rolling-ops/{leader_id}")
    await ops_test.model.block_until(lambda: app.status in ("error", "blocked", "active"))
    assert app.status == "active"
    unit_id = await get_a_non_leader_unit_id()
    await _assert_restart_1_unit(ops_test, app, unit_id)


@pytest.mark.parametrize(
    [
        (pytest.param(series, marks=pytest.mark.group(f"{series}: stop agent")))
        for series in TARGET_SERIES
    ],
)
@pytest.mark.abort_on_fail
async def test_stop_agent_and_restart(ops_test: OpsTest, app) -> None:
    # Remove a non-leader unit
    unit_id = await get_a_non_leader_unit_id()
    subprocess.run(
        ["juju", "ssh", f"rolling-ops/{unit_id}", "sudo", "systemctl", "stop", "jujud*"]
    )
    subprocess.run(
        ["juju", "ssh", f"rolling-ops/{unit_id}", "sudo", "systemctl", "disable", "jujud*"]
    )
    # Wait a few seconds for the unit to be marked as down
    await asyncio.sleep(2 * UPDATE_STATUS_IN_SECONDS)

    for i in range(NUM_UNITS):
        if i == unit_id:
            continue
        await _assert_restart_1_unit(ops_test, app, i)