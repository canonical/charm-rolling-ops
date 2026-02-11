# src/rollingops_async.py
import logging
import os
import signal
import time

import subprocess
from pathlib import Path
from sys import version_info
from typing import Optional

from ops.charm import CharmBase, CharmEvents
from ops.framework import EventBase, EventSource, Object
from ops.model import ActiveStatus

logger = logging.getLogger(__name__)

LOG_FILE_PATH = "/var/log/rollingops_worker.log"


class RollingOpGrantedEvent(EventBase):
    """Custom event emitted when the background worker grants the lock."""



class RollingOpsCharmEvents(CharmEvents):
    rollingop_granted = EventSource(RollingOpGrantedEvent)

class RollingOpsAsyncWorker(Object):
    """Spawns and manages the external rolling-ops worker process."""

    def __init__(self, charm: CharmBase, peers_relation_name: str, run_cmd: str):
        super().__init__(charm, "rollingops-async-worker")
        self._charm = charm
        self._peers_name = peers_relation_name
        self._run_cmd = run_cmd

    @property
    def _peers(self):
        return self._charm.model.get_relation(self._peers_name)

    def start(self, callback_name: str, acquire_delay: int = 5):
        """Start the worker process if not already running."""
        if not isinstance(self._charm.unit.status, ActiveStatus) or self._peers is None:
            return

        unit_data = self._peers.data[self._charm.unit]

        # If already running, check PID still alive
        pid_str = unit_data.get("rollingops-worker-pid", "")
        if pid_str:
            try:
                os.kill(int(pid_str), 0)
                logger.info("RollingOps worker already running with PID %s", pid_str)
                return
            except OSError:
                logger.info("Stale worker PID %s; will respawn", pid_str)

        logger.info("Starting RollingOps worker process")

        # Remove JUJU_CONTEXT_ID so juju-run works from the spawned process
        new_env = os.environ.copy()
        new_env.pop("JUJU_CONTEXT_ID", None)

        # Ensure the charm venv site-packages are on PYTHONPATH (same trick as Postgres)
        for loc in new_env.get("PYTHONPATH", "").split(":"):
            path = Path(loc)
            venv_path = (
                path
                / ".."
                / "venv"
                / "lib"
                / f"python{version_info.major}.{version_info.minor}"
                / "site-packages"
            )
            if path.stem == "lib":
                new_env["PYTHONPATH"] = f"{venv_path.resolve()}:{new_env['PYTHONPATH']}"
                break

        # Persist the "what should I run" selection for the unit
        unit_data.update({"rollingops-callback": callback_name})

        # Spawn the worker (simulation: sleeps then dispatches event)
        worker = self._charm.charm_dir / "scripts" / "rollingops_worker.py"

        proc = subprocess.Popen(
            [
                "/usr/bin/python3",
                "-u",
                str(worker),
                "--acquire-delay", str(acquire_delay),
                "--run-cmd", self._run_cmd,
                "--unit-name", self._charm.unit.name,
                "--charm-dir", str(self._charm.charm_dir),
                "--event-name", "rollingop_granted",
            ],
            cwd=str(self._charm.charm_dir),
            stdout=open(LOG_FILE_PATH, "a"),
            stderr=subprocess.STDOUT,
            env=new_env,
        )
        pid = proc.pid


        unit_data.update({"rollingops-worker-pid": str(pid)})
        logger.info("Started RollingOps worker process with PID %s", pid)

    def stop(self):
        """Stop the running worker process if it exists."""
        if self._peers is None:
            return
        unit_data = self._peers.data[self._charm.unit]
        pid_str = unit_data.get("rollingops-worker-pid", "")
        if not pid_str:
            return

        pid = int(pid_str)
        try:
            os.kill(pid, signal.SIGINT)
            logger.info("Stopped RollingOps worker process PID %s", pid)
        except OSError:
            pass
        unit_data.update({"rollingops-worker-pid": ""})
