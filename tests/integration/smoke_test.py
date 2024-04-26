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

import asyncio
import json
import logging
import subprocess

import pytest
from juju.action import Action
from juju.application import Application
from juju.model import JujuAPIError, Model
from juju.unit import Unit
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


def get_restart_type(unit: Unit, model_name: str) -> str:
    show_unit_json = subprocess.check_output(
        f"JUJU_MODEL={model_name} juju show-unit {unit.name} --format json",
        stderr=subprocess.PIPE,
        shell=True,
        universal_newlines=True,
    )
    show_unit_dict = json.loads(show_unit_json)
    restart_type = show_unit_dict[unit.name]["relation-info"][0]["local-unit"]["data"][
        "restart-type"
    ]

    return restart_type


@pytest.mark.abort_on_fail
@pytest.mark.group(1)
async def test_smoke(ops_test: OpsTest):
    """Basic smoke test following the default callback implementation.

    Verify that we can deploy, and seem to be able to run a rolling op.
    """
    # to spare the typechecker errors
    assert ops_test.model
    assert ops_test.model_full_name
    model: Model = ops_test.model
    model_full_name: str = ops_test.model_full_name

    # Deploy, and verify deployment
    charm = await ops_test.build_charm(".")
    await asyncio.gather(ops_test.model.deploy(charm, application_name="rolling-ops", num_units=3))

    # to spare the typechecker errors
    assert model.applications["rolling-ops"]
    app: Application = model.applications["rolling-ops"]

    await ops_test.model.block_until(lambda: app.status in ("error", "blocked", "active"))
    assert app.status == "active"

    for action_type in ["restart", "custom-restart"]:
        # Run the restart, with a delay to alleviate timing issues.
        for unit in app.units:
            logger.info(f"{action_type} - {unit.name}")
            action: Action = await unit.run_action(action_type, delay=10)
            await action.wait()
            assert (action.results.get("return-code", None) == 0) or (
                action.results.get("Code", None) == "0"
            )

        await model.block_until(lambda: app.status in ("maintenance", "error"), timeout=60)
        assert app.status != "error"

        await model.wait_for_idle(status="active", timeout=600)

        for unit in app.units:
            restart_type = get_restart_type(unit=unit, model_name=model_full_name)
            assert restart_type == action_type


@pytest.mark.abort_on_fail
@pytest.mark.group(1)
async def test_smoke_single_unit(ops_test):
    """Basic smoke test, on a single unit.

    Verify that deployment and rolling ops succeed for a single unit.
    """
    # to spare the typechecker errors
    assert ops_test.model
    assert ops_test.model_full_name
    model: Model = ops_test.model
    model_full_name: str = ops_test.model_full_name

    assert model.applications["rolling-ops"]
    app: Application = model.applications["rolling-ops"]

    # Scale down to one unit
    try:
        await app.scale(1)
    except JujuAPIError:  # handling vm vs k8s
        await app.destroy_units("rolling-ops/2", "rolling-ops/1")

    # wait for unit to be ready
    await model.block_until(lambda: len(app.units) == 1)
    assert app.status == "active"

    for action_type in ["restart", "custom-restart"]:
        # Run the restart, with a delay to alleviate timing issues.
        logger.info(f"{action_type} - {app.units[0].name}")
        action: Action = await app.units[0].run_action(action_type, delay=30)

        await model.block_until(lambda: app.status in ("maintenance", "error"), timeout=60)
        assert app.status != "error"

        await action.wait()
        assert (action.results.get("return-code", None) == 0) or (
            action.results.get("Code", None) == "0"
        )

        await model.wait_for_idle(status="active", timeout=600)

        for unit in app.units:
            restart_type = get_restart_type(unit=unit, model_name=model_full_name)
            assert restart_type == action_type


@pytest.mark.abort_on_fail
@pytest.mark.group(1)
async def test_scale_up(ops_test: OpsTest):
    """Scale the application back up, restart again."""
    # to spare the typechecker errors
    assert ops_test.model
    assert ops_test.model_full_name
    model: Model = ops_test.model
    model_full_name: str = ops_test.model_full_name

    # to spare the typechecker errors
    assert model.applications["rolling-ops"]
    app: Application = model.applications["rolling-ops"]

    try:
        await app.scale(3)
    except JujuAPIError:  # handling vm vs k8s
        await app.add_units(2)

    await model.wait_for_idle(status="active", timeout=600)

    # Run the restart for all units
    for unit in app.units:
        logger.info(f"restart - {unit.name}")
        action: Action = await unit.run_action("restart", delay=10)
        await action.wait()
        assert (action.results.get("return-code", None) == 0) or (
            action.results.get("Code", None) == "0"
        )

    await model.block_until(lambda: app.status in ("maintenance", "error"), timeout=60)
    assert app.status != "error"

    await model.wait_for_idle(status="active", timeout=600)

    for unit in app.units:
        restart_type = get_restart_type(unit=unit, model_name=model_full_name)
        assert restart_type == "restart"


@pytest.mark.abort_on_fail
@pytest.mark.group(1)
async def test_on_delete(ops_test: OpsTest):
    """Basic restart followed by premature app deletion.

    The lock should not block the application from getting removed completely.
    """
    # to spare the typechecker errors
    assert ops_test.model
    assert ops_test.model_full_name
    model: Model = ops_test.model

    # to spare the typechecker errors
    assert model.applications["rolling-ops"]
    app: Application = model.applications["rolling-ops"]

    # Run the restart, with a big delay to stall units during the operation.
    # We don't wait for the restart operation to finish before removing it
    for unit in app.units:
        logger.info(f"restart - {unit.name}")
        await unit.run_action("restart", delay=60)

    await model.block_until(lambda: app.status in ("maintenance", "error"), timeout=60)
    assert app.status != "error"

    for unit in app.units:
        logger.info(f"removing unit - {unit.name}")
        await app.destroy_unit(unit.name)

    await model.block_until(lambda: len(app.units) == 0, timeout=600)
    assert app.status != "error"
    await ops_test.model.remove_application("rolling-ops", block_until_done=True)
