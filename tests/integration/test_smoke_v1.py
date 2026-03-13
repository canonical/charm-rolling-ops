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


#@pytest.fixture(scope="module", autouse=True)
#def copy_rolling_ops_library_into_charm(ops_test: OpsTest):
#    """Copy the data_interfaces library to the different charm folder."""
#    library_path = "lib/charms/rolling_ops/v1/rollingops.py"
#    install_path = "tests/charms/v1/" + library_path
#    shutil.copyfile(library_path, install_path)

#    library_path = "lib/charms/data_platform_libs/v0/data_interfaces.py"
#    install_path = "tests/charms/v1/" + library_path
#    shutil.copyfile(library_path, install_path)


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

    await ops_test.model.deploy(
        "self-signed-certificates",
        application_name="self-signed-certificates",
        channel="1/stable"
    )
    await ops_test.model.deploy("charmed-etcd", application_name="etcd", channel="3.6/stable")

    await ops_test.model.integrate("etcd:client-certificates","self-signed-certificates:certificates")
    await ops_test.model.integrate("rollingops:etcd","etcd:etcd-client")

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

    for transition in expected:
        assert transition in restart_events





