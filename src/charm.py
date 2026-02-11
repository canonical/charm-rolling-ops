import logging
import time

from ops import ActiveStatus, CharmBase, main
from ops.framework import StoredState


from rollingops_async import RollingOpsCharmEvents, RollingOpsAsyncWorker

logger = logging.getLogger(__name__)


class CharmRollingOpsCharm(CharmBase):
    on = RollingOpsCharmEvents()
    _stored = StoredState()

    def __init__(self, *args):
        super().__init__(*args)

        self._stored.set_default(delay=None)
        self.restart_manager = RollingOpsManager(
            charm=self, relation="restart", callback=self._restart
        )

        # peer relation name assumed "restart" here for demo
        # run_cmd is the path to juju-run (often /usr/bin/juju-run)
        run_cmd = (
            "/usr/bin/juju-exec" if self.model.juju_version.major > 2 else "/usr/bin/juju-run"
        )
        self.worker = RollingOpsAsyncWorker(self, peers_relation_name="restart", run_cmd=run_cmd)

        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.restart_action, self._on_restart_action)
        self.framework.observe(self.on.custom_restart_action, self._on_custom_restart_action)
        self.framework.observe(self.on.rollingop_granted, self._on_rollingop_granted)
        self.framework.observe(self.on.patty_emitted, self._on_patty_emitted)

    def _on_install(self, event):
        self.unit.status = ActiveStatus()

    def _restart(self, event):
        logger.info("RUNNING _restart")
        if self._stored.delay:
            time.sleep(int(self._stored.delay))
        logger.info("_restart done")

    def _custom_restart(self, event):
        logger.info("RUNNING _custom_restart")
        if self._stored.delay:
            time.sleep(int(self._stored.delay))
        logger.info("_custom_restart done")

    def _on_restart_action(self, event):
        self._stored.delay = event.params.get("delay")
        self.worker.start(callback_name="_restart", acquire_delay=5)
        event.set_results({"status": "requested", "callback": "_restart"})

    def _on_custom_restart_action(self, event):
        self._stored.delay = event.params.get("delay")
        self.worker.start(callback_name="_custom_restart", acquire_delay=5)
        event.set_results({"status": "requested", "callback": "_custom_restart"})

    def _on_rollingop_granted(self, event):
        # Read callback selection from peer databag
        rel = self.model.get_relation("restart")
        cb_name = rel.data[self.unit].get("rollingops-callback", "")
        logger.info("rollingop_granted: callback=%s", cb_name)

        cb = getattr(self, cb_name, None)
        if not cb:
            logger.warning("No valid callback configured; ignoring")
            return

        cb(event)




if __name__ == "__main__":
    main(CharmRollingOpsCharm)
