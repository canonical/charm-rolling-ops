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

import asyncio
import json
import logging
import subprocess
from datetime import datetime, timezone
from typing import Optional
from . import architecture

import pytest
from juju.action import Action
from juju.application import Application
from juju.model import Model
from juju.unit import Unit
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S.%fZ"


def _parse_timestamp(timestamp: str) -> Optional[datetime]:
    """Parse timestamp string. Return 'now' on errors to avoid selecting invalid timestamps."""
    try:
        dt = datetime.strptime(timestamp, TIMESTAMP_FORMAT)
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _get_unit_data(unit: Unit, model_name: str):
    show_unit_json = subprocess.check_output(
        f"JUJU_MODEL={model_name} juju show-unit {unit.name} --format json",
        stderr=subprocess.PIPE,
        shell=True,
        universal_newlines=True,
    )
    show_unit_dict = json.loads(show_unit_json)
    return show_unit_dict[unit.name]["relation-info"][0]["local-unit"]["data"]


def get_executed_at(unit: Unit, model_name: str) -> Optional[datetime]:
    unit_data = _get_unit_data(unit, model_name)
    executed_at = unit_data.get("executed_at", "")
    return _parse_timestamp(executed_at)


def get_operations_queue(unit: Unit, model_name: str) -> list[dict]:
    """Return list of operation dicts from the unit databag 'operations' string."""
    unit_data = _get_unit_data(unit, model_name)
    operations_raw = unit_data.get("operations", "")
    if not operations_raw:
        return []
    outer = json.loads(operations_raw)
    ops = [json.loads(item) for item in outer]
    return ops


@pytest.mark.abort_on_fail
async def test_smoke(ops_test: OpsTest):
    """Basic smoke test following the default callback implementation.

    Verify that we can deploy, and seem to be able to run a rolling op.
    """
    assert ops_test.model
    assert ops_test.model_full_name
    model: Model = ops_test.model
    model_full_name: str = ops_test.model_full_name

    charm = await ops_test.build_charm(".")
    charm = f"./rolling-ops_ubuntu@22.04-{architecture.architecture}.charm"
    await asyncio.gather(ops_test.model.deploy(charm, application_name="rolling-ops", num_units=3))

    assert model.applications["rolling-ops"]
    app: Application = model.applications["rolling-ops"]

    await ops_test.model.block_until(lambda: app.status in ("error", "blocked", "active"))
    assert app.status == "active"

    action_type = "restart"
    # Run the restart, with a delay to alleviate timing issues.
    for unit in app.units:
        logger.info(f"Running {action_type} - {unit.name}")
        action: Action = await unit.run_action(action_type, delay=10)
        await action.wait()
        assert (action.results.get("return-code", None) == 0) or (
            action.results.get("Code", None) == "0"
        )
        await model.wait_for_idle(status="active", timeout=600)
        assert get_executed_at(unit=unit, model_name=model_full_name)


@pytest.mark.abort_on_fail
async def test_dedupe_when_other_unit_holds_lock(ops_test: OpsTest):
    assert ops_test.model
    assert ops_test.model_full_name
    model: Model = ops_test.model
    model_full_name: str = ops_test.model_full_name

    app = model.applications["rolling-ops"]
    unit_a, unit_b = app.units[0], app.units[1]

    long_delay = 60
    act_a = await unit_a.run_action("restart", delay=long_delay)

    await model.block_until(
        lambda: unit_a.workload_status == "maintenance",
        timeout=30,
    )
    logger.info(f"Succesive lock request on {unit_b}.")
    for _ in range(3):
        await unit_b.run_action("restart", delay=0)
        await asyncio.sleep(5)

    ops = get_operations_queue(unit=unit_b, model_name=model_full_name)

    assert len(ops) == 1, f"expected deduped queue of length 1, got {len(ops)}: {ops}"

    logger.info(f"Waiting for {unit_a} to finish execution.")
    await act_a.wait()
    await model.wait_for_idle(apps=["rolling-ops"], status="active", timeout=600)
    executed_a = get_executed_at(unit=unit_a, model_name=model_full_name)
    executed_b = get_executed_at(unit=unit_b, model_name=model_full_name)
    assert executed_b > executed_a
