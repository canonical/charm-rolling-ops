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

import pytest
from juju.application import Application
from juju.model import Model
from pytest_operator.plugin import OpsTest


@pytest.fixture
async def model(ops_test: OpsTest) -> Model:
    return ops_test.model


@pytest.fixture
async def app(model) -> Application:
    return model.applications["rolling-ops"]
