#!/usr/bin/env python3
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

"""Sample charm using the rolling ops library."""

import json
import logging
import time

from charms.rolling_ops.v0.rollingops import Locks, RollingOpsManager
from ops.charm import CharmBase
from ops.framework import StoredState
from ops.main import main
from ops.model import ActiveStatus

logger = logging.getLogger(__name__)


class RollingOpsManagerWithTimestamps(RollingOpsManager):
    """Extends the RollingOpsManager to add timestamps to the lock data."""

    _stored = StoredState()

    def __init__(self, charm, relation, callback):
        super().__init__(charm, relation, callback)
        self._stored.set_default(timestamps={})

    def _on_process_locks(self: CharmBase, event):
        """Extends the base class to add timestamps to the lock data."""
        if not self.model.unit.is_leader():
            return

        pending = []

        for lock in Locks(self):
            if lock.is_held():
                # One of our units has the lock -- return without further processing.
                return

            if lock.unit.name not in self._stored.timestamps:
                self._stored.timestamps[lock.unit.name] = {}

            if lock.release_requested():
                # This lock will be cleared:
                self._stored.timestamps[lock.unit.name] = {
                    **self._stored.timestamps[lock.unit.name],
                    **{"released": time.time()},
                }

            if lock.is_pending():
                if lock.unit == self.model.unit:
                    # Always run on the leader last.
                    pending.insert(0, lock)
                else:
                    pending.append(lock)

        if pending:
            # This unit will receive the lock
            lock = pending[-1]
            self._stored.timestamps[lock.unit.name] = {
                **self._stored.timestamps[lock.unit.name],
                **{"requested": time.time()},
            }
            if lock.unit == self.model.unit:
                # Now, this is the unit that will run this lock.
                # It happens the leader runs it right away, so mark it as cleared as well.
                self._stored.timestamps[lock.unit.name] = {
                    **self._stored.timestamps[lock.unit.name],
                    **{"released": time.time()},
                }

        # Now, we can run the base class method.
        super()._on_process_locks(event)


class CharmRollingOpsCharm(CharmBase):
    """Charm the service."""

    _stored = StoredState()

    def __init__(self, *args):
        super().__init__(*args)

        self.restart_manager = RollingOpsManagerWithTimestamps(
            charm=self, relation="restart", callback=self._restart
        )

        self.framework.observe(self.on.install, self._on_install)

        self.framework.observe(self.on.get_lock_status_action, self._on_get_lock_status)
        self.framework.observe(self.on.share_useless_data_action, self._share_useless_data)

        self.framework.observe(self.on.restart_action, self._on_restart_action)
        self.framework.observe(self.on.custom_restart_action, self._on_custom_restart_action)

        # Sentinel for testing (omit from production charms)
        self._stored.set_default(restarted=False)
        self._stored.set_default(delay=None)

    def _on_get_lock_status(self, event):
        if not self.unit.is_leader():
            event.fail("This action can only be run by the leader")
            return

        if not self.model.get_relation(self.restart_manager.name):
            event.fail("No restart relation found")
            return

        result = []
        for lock in Locks(self.restart_manager):
            result.append(
                json.dumps(
                    {
                        "unit-name": lock.unit.name,
                        "status": str(lock._state),
                        "useless-data": lock.relation.data[self.unit].get("useless-data", 0),
                    }
                )
            )
        event.set_results(
            {
                "lock-status-overall": result,
                "timestamps": str(
                    self.restart_manager._stored.timestamps._stored_data.snapshot().get(
                        "timestamps", {}
                    )
                ),
            }
        )

    def _share_useless_data(self, event):
        if not self.model.get_relation(self.restart_manager.name):
            event.fail("No restart relation found")
            return
        reldata = self.model.get_relation(self.restart_manager.name).data[self.unit]
        reldata["useless-data"] = str(int(reldata.get("useless-data", 0)) + 1)

    def _restart(self, event):
        # In a production charm, we'd perhaps import the systemd library, and run
        # systemd.restart_service.  Here, we just set a sentinel in our stored state, so
        # that we can run our tests.
        if self._stored.delay:
            time.sleep(int(self._stored.delay))
        self._stored.restarted = True

        self.model.get_relation(self.restart_manager.name).data[self.unit].update(
            {"restart-type": "restart"}
        )

    def _custom_restart(self, event):
        if self._stored.delay:
            time.sleep(int(self._stored.delay))

        self.model.get_relation(self.restart_manager.name).data[self.unit].update(
            {"restart-type": "custom-restart"}
        )

    def _on_install(self, event):
        self.unit.status = ActiveStatus()

    def _on_restart_action(self, event):
        self._stored.delay = event.params.get("delay")
        self.on[self.restart_manager.name].acquire_lock.emit()

    def _on_custom_restart_action(self, event):
        self._stored.delay = event.params.get("delay")
        self.on[self.restart_manager.name].acquire_lock.emit(callback_override="_custom_restart")


if __name__ == "__main__":
    main(CharmRollingOpsCharm)
