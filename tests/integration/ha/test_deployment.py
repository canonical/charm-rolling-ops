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

import pytest
from juju.model import Model
from pytest_operator.plugin import OpsTest

from .helpers import (
    GROUP_MARKS,
    MODEL_CONFIG,
    NUM_UNITS,
    TARGET_SERIES,
    UPDATE_STATUS_IN_SECONDS,
    _assert_restart_1_unit,
    get_a_non_leader_unit_id,
    get_leader_unit_id,
)

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "group_name",
    [
        (
            # pytest.param(series),
            pytest.param(
                "{}-{}".format(series, GROUP_MARKS[i]),
                marks=pytest.mark.group("{}-{}".format(series, GROUP_MARKS[i])),
            ),
        )
        for series in TARGET_SERIES
        for i in range(len(GROUP_MARKS))
    ],
)
@pytest.mark.abort_on_fail
@pytest.mark.skip_if_deployed
async def test_build_and_deploy(ops_test: OpsTest, group_name, model, app) -> None:
    my_charm = await ops_test.build_charm(".")
    await ops_test.model.set_config(MODEL_CONFIG)
    await ops_test.model.deploy(my_charm, num_units=NUM_UNITS)  # , series=series)
    model: Model = ops_test.model
    app = model.applications["rolling-ops"]

    await ops_test.model.block_until(lambda: app.status in ("error", "blocked", "active"))
    assert app.status == "active"


# @pytest.mark.parametrize(
#     "series",
#     [
#         (pytest.param(series, marks=pytest.mark.group(f"{series}-{GROUP_MARKS[0]}")))
#         for series in TARGET_SERIES
#     ],
# )
# @pytest.mark.abort_on_fail
# async def test_restart_1_unit(ops_test: OpsTest, series, app) -> None:
#     unit_id = await get_a_non_leader_unit_id()
#     await _assert_restart_1_unit(ops_test, app, unit_id)


# @pytest.mark.parametrize(
#     "series",
#     [
#         (pytest.param(series, marks=pytest.mark.group(f"{series}-{GROUP_MARKS[1]}")))
#         for series in TARGET_SERIES
#     ],
# )
# @pytest.mark.abort_on_fail
# async def test_restart_all_units(ops_test: OpsTest, series, app) -> None:
#     for i in range(NUM_UNITS):
#         await _assert_restart_1_unit(ops_test, app, i)


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_remove_and_restart(ops_test: OpsTest, app) -> None:
    # Remove a non-leader unit
    unit_id = await get_a_non_leader_unit_id()
    await app.units[unit_id].remove()

    await ops_test.model.block_until(lambda: app.status in ("error", "blocked", "active"))
    assert app.status == "active"
    unit_id = await get_a_non_leader_unit_id()
    await _assert_restart_1_unit(ops_test, app, unit_id)


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_remove_leader_and_restart(ops_test: OpsTest, app) -> None:
    # Remove a non-leader unit
    leader_id = await get_leader_unit_id()
    await app.destroy_units(f"rolling-ops/{leader_id}")
    await ops_test.model.block_until(lambda: app.status in ("error", "blocked", "active"))
    assert app.status == "active"
    unit_id = await get_a_non_leader_unit_id()
    await _assert_restart_1_unit(ops_test, app, unit_id)


@pytest.mark.group(1)
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
