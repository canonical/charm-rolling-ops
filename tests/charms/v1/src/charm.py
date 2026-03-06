#!/usr/bin/env python3
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

"""Sample charm using the rolling ops library."""

import logging
import time

from charms.rolling_ops.v1.rollingops import OperationResult, RollingOpsManagerV1
from ops import CharmBase, main
from ops.model import ActiveStatus, MaintenanceStatus, WaitingStatus

logger = logging.getLogger(__name__)


class CharmRollingOpsCharmV1(CharmBase):
    """Charm the service."""

    def __init__(self, *args):
        super().__init__(*args)

        callback_targets = {
            "_restart": self._restart,
            "_failed_restart": self._failed_restart,
            "_deferred_restart": self._deferred_restart,
        }

        self.restart_manager = RollingOpsManagerV1(
            charm=self, relation_name="restart", callback_targets=callback_targets
        )

        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.restart_action, self._on_restart_action)
        self.framework.observe(self.on.failed_restart_action, self._on_failed_restart_action)
        self.framework.observe(self.on.deferred_restart_action, self._on_deferred_restart_action)

    def _restart(self, delay: int = 0):
        # In a production charm, we'd perhaps import the systemd library, and run
        # systemd.restart_service.  Here, we just set a sentinel in our stored state, so
        # that we can run our tests.
        logger.info("Starting restart operation")
        self.model.unit.status = MaintenanceStatus("Executing _restart operation")
        time.sleep(int(delay))
        self.model.unit.status = ActiveStatus()

    def _failed_restart(self, delay: int = 0):
        logger.info("Starting failed restart operation")
        self.model.unit.status = MaintenanceStatus("Executing _failed_restart operation")
        time.sleep(int(delay))
        self.model.unit.status = MaintenanceStatus("Rolling restart operation failed")
        return OperationResult.RETRY_RELEASE

    def _deferred_restart(self, delay: int = 0):
        logger.info("Starting deferred restart operation")
        self.model.unit.status = MaintenanceStatus("Executing _deferred_restart operation")
        time.sleep(int(delay))
        self.model.unit.status = MaintenanceStatus("Rolling restart operation failed")
        return OperationResult.RETRY_HOLD

    def _on_install(self, event):
        self.unit.status = ActiveStatus()

    def _on_restart_action(self, event):
        self.model.unit.status = WaitingStatus("Awaiting _restart operation")
        self.restart_manager.request_async_lock(
            callback_id="_restart", kwargs={"delay": event.params.get("delay")}
        )

    def _on_failed_restart_action(self, event):
        self.model.unit.status = WaitingStatus("Awaiting _failed_restart operation")
        self.restart_manager.request_async_lock(
            callback_id="_failed_restart",
            kwargs={"delay": event.params.get("delay")},
            max_retry=event.params.get("max-retry", None),
        )

    def _on_deferred_restart_action(self, event):
        self.model.unit.status = WaitingStatus("Awaiting _deferred_restart operation")
        self.restart_manager.request_async_lock(
            callback_id="_deferred_restart",
            kwargs={
                "delay": event.params.get("delay"),
            },
            max_retry=event.params.get("max-retry", None),
        )


if __name__ == "__main__":
    main(CharmRollingOpsCharmV1)
