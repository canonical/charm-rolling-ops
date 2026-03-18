# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.


from typing import Literal

import pytest
from _pytest.config.argparsing import Parser

Substrate = Literal["lxd", "microk8s"]


@pytest.fixture(scope="session")
def substrate(request) -> Substrate:
    return request.config.getoption("--substrate")


def pytest_addoption(parser: Parser):
    parser.addoption(
        "--substrate",
        action="store",
        help="Substrate to test, either lxd or microk8s",
        choices=("lxd", "microk8s"),
        default="lxd",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "skip_if_substrate(substrate): skip test for the given substrate"
    )
