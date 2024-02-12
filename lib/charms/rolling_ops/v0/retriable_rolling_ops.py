# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Class for retriable rolling ops.

There are two types of locks needed:
1) The standard rolling-ops library
2) Workload-specific lock - used in special cases where retrying to acquire
   the lock again via relations is not possible, e.g. storage-detaching.

The former is used to control rolling operations across a cluster, where
we can reliably use the peer relation to orchestrate these activities. The
leader unit will keep the control and will grant the lock to the next unit
in the relation.

The latter is used to control the removal of units from the cluster. In this
case, the sensitive operations happen in the storage-detaching event, which
cannot be deferred or abandoned. This event will trigger a series of steps
that will flush data to disk and exclude the unit from any voting/allocation.

As everything happening in the storage-detaching must be atomic, we cannot
rely on the peer relation and events being triggered later on in the process
in other units. We must use the opensearch itself to store the lock info.
That assures any unit can access locking information at any time, even during
a storage-detaching event on a peer unit.

We must avoid having both lock types conceeding locks at the same time.
We also must avoid providing locks when the cluster has nodes that are not
running its service or should be considered "out of cluster".

For that, the charm leader needs to also check if remote units are up and
running and manage the lock accordingly. This monitoring should start once
the unit has finished its start-up process.

-------------------------------------------------------------------------------

The lifecycle of this lock system is:

            +--> ACQUIRE_BUT_SVC_NOT_RESPONSIVE > GRANTED ...
            |
NOT_STARTED +--> ACQUIRE > GRANTED +--> RELEASE > IDLE > DEPARTING (END)
                                   |
                                   +--> RETRY_LOCK > RELEASE > ...

A given node starts without its starting flag set in the service relation.
Once the unit has finished its start-up process, it sets the flag "has_started"

This flag allows the charm leader to separate nodes that should be considered
as "OFF" because their service is non-responsive/missing a information, from
nodes that are still starting and the service is not up yet. The charm leader
will not consider nodes that do not have "has_started" not set to "True".

Once the service is up, the node can start requesting locks under the Retriable
Rolling Ops. Normally, the system behaves just like the RollingOpsManager.

If a node that has been granted the lock cannot proceed with its RunWithLock,
the node can abandon the lock and try to reacquire it later. For that, it is
enough to run a "defer" in the RunWithLock event OR raise:
RollingOpsRetryLockLaterException.

The charm leader will process any acquire requests from nodes as usual. If the
running unit defers or raises an error, the _on_run_with_lock method will capture
that, release the lock and log the error.

The charm leader will increase its "retrial_count" and pass the lock to the next
requestor. The unit can then reissue the lock later. That is done automatically at
the constructor of the RetriableRollingOpsManager on that unit.

The charm leader selects the next unit to receive the lock based on the following order:
1) Do we have any locks with: ACQUIRE_BUT_SVC_NOT_RESPONSIVE
2) Do we have any locks with: RETRY_LOCK
3) Do we have any locks with: ACQUIRE (all the remaining)

In particular, on (3), the last unit to be served is the leader if it has requested for
the lock. The lock will be granted if the WorkloadLockManager is implemented, not
departing and there are no units with the service stopped in the cluster.

-------------------------------------------------------------------------------

"""

import logging
from enum import Enum

from charms.rolling_ops.v0.rollingops import Lock, Locks, LockState, RollingOpsManager


# The unique Charmhub library identifier, never change it
LIBID = "78caa49d36f7417b9c8750daa79627ce"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 1


logger = logging.getLogger(__name__)


class RetriableLockState(Enum):
    """Reports the status of a given node for locking purposes.

    Besides the original: ACQUIRE, RELEASE, GRANTED, IDLE, also count the cases:
    - DEPARTING: node has requested a lock via workload manager during storage-detaching
    - ACQUIRE_BUT_SVC_NOT_RESPONSIVE: requested the lock while its service is stopped
    - STOPPED_BUT_NOT_REQUESTED: the service is stopped, but the lock was not requested
    - RETRY_LOCK: the lock should be retried later
    """

    ACQUIRE = "acquire"
    RELEASE = "release"
    GRANTED = "granted"
    IDLE = "idle"
    DEPARTING = "departing"
    RETRY_LOCK = "retry-lock"
    ACQUIRE_BUT_SVC_NOT_RESPONSIVE = "acquire-but-svc-not-responsive"
    STOPPED_BUT_NOT_REQUESTED = "stopped-but-not-requested"


class RollingOpsRetryLockLaterException(Exception):
    """Exception thrown when the lock should be retried later."""


class RetriableLock(Lock):
    """Class for controlling the locks in OpenSearch Charm."""

    def __init__(self, manager, unit):
        super().__init__(manager, unit=unit)
        self.manager = manager
        self.charm = manager.charm
        # Run it once, so we can store the status of the unit.
        if self.charm.unit == self.unit and self.unit_is_up:
            self.started()

    @property
    def unit_is_up(self) -> bool:
        """Checks if the unit is up."""
        return self.manager.workload_manager.is_node_up(self.unit)

    @property
    def _state(self) -> RetriableLockState:
        """Gets the lock state."""
        if self.manager.workload_manager.is_departing():
            # Simple case, we will not process locks until the unit is removed.
            return RetriableLockState.DEPARTING

        app_state = RetriableLockState(super()._state.value)
        if app_state in [RetriableLockState.RELEASE, RetriableLockState.GRANTED]:
            return app_state

        # Now, we need to figure out if the extra status of the lock.
        # Currently, we know the lock is either ACQUIRE or IDLE.
        if self.retrial_count > 0:
            # The unit is requesting another retry.
            return RetriableLockState.RETRY_LOCK

        # Is the unit down?
        if not self.unit_is_up:
            if app_state == RetriableLockState.ACQUIRE:
                # The unit is requesting the lock.
                return RetriableLockState.ACQUIRE_BUT_SVC_NOT_RESPONSIVE
            return RetriableLockState.STOPPED_BUT_NOT_REQUESTED

        # Return can either be originals ACQUIRE or IDLE
        if app_state == RetriableLockState.ACQUIRE:
            # The unit is requesting the lock.
            return RetriableLockState.ACQUIRE
        return RetriableLockState.IDLE

    @_state.setter
    def _state(self, s: LockState):
        """Sets the lock state.

        Although the lock may have more states, these are calculated at _state call.
        The states to be stored remains the same as the parent class.
        """
        state = RetriableLockState(s.value)
        if state in [
            RetriableLockState.ACQUIRE,
            RetriableLockState.DEPARTING,
            RetriableLockState.RETRY_LOCK,
            RetriableLockState.ACQUIRE_BUT_SVC_NOT_RESPONSIVE,
        ]:
            self.relation.data[self.unit].update({"state": LockState.ACQUIRE.value})
        elif state == RetriableLockState.RELEASE:
            self.relation.data[self.unit].update({"state": LockState.RELEASE.value})
        elif state == RetriableLockState.GRANTED:
            self.relation.data[self.app].update({str(self.unit): LockState.GRANTED.value})
        elif state in [
            RetriableLockState.IDLE,
            RetriableLockState.STOPPED_BUT_NOT_REQUESTED,
        ]:
            self.relation.data[self.app].update({str(self.unit): LockState.IDLE.value})

    def is_blocked(self) -> bool:
        """Method for checking if the lock is blocked."""
        return self.has_started() and (
            self._state == RetriableLockState.DEPARTING
            or self._state == RetriableLockState.STOPPED_BUT_NOT_REQUESTED
        )

    def is_held(self):
        """This unit holds the lock."""
        return self._state == RetriableLockState.GRANTED

    def release_requested(self):
        """A unit has reported that they are finished with the lock."""
        return self._state == RetriableLockState.RELEASE

    def is_pending(self):
        """Is this unit waiting for a lock?"""
        return self._state in [
            RetriableLockState.ACQUIRE,
            RetriableLockState.DEPARTING,
            RetriableLockState.RETRY_LOCK,
            RetriableLockState.ACQUIRE_BUT_SVC_NOT_RESPONSIVE,
        ]

    def has_started(self) -> bool:
        """Method for checking if the unit has started."""
        return self.relation.data[self.unit].get("has_started") == "True"

    def started(self):
        """Sets the started flag.

        Should be called once the unit has finished its start process.
        """
        self.relation.data[self.unit]["has_started"] = "True"

    def retry(self) -> bool:
        """Method for checking if the lock should be retried."""
        return self._state == RetriableLockState.RETRY_LOCK

    @property
    def retrial_count(self) -> int:
        """Method for getting the retrial count."""
        return int(self.relation.data[self.unit].get(RetriableLockState.RETRY_LOCK.value, 0))

    @retrial_count.setter
    def retrial_count(self, count: int):
        """Method for getting the retrial count."""
        self.relation.data[self.unit][RetriableLockState.RETRY_LOCK.value] = str(count)

    def acquire_with_stopped_service(self) -> bool:
        """Method for checking if the lock is acquired with the service stopped."""
        return self._state == RetriableLockState.ACQUIRE_BUT_SVC_NOT_RESPONSIVE


class RetriableLocks(Locks):
    """Generator that returns a list of locks."""

    def __init__(self, manager):
        super().__init__(manager)

    def __iter__(self):
        """Yields a lock for each unit we can find on the relation."""
        for unit in self.units:
            yield RetriableLock(self.manager, unit=unit)


class WorkloadLockManager:
    def is_departing(self) -> bool:
        """Checks if the workload lock has been assigned and a unit is departing."""
        raise NotImplementedError

    def is_node_up(self, unit) -> bool:
        """Checks if a unit's workload is running."""
        raise NotImplementedError


class RetriableRollingOpsManager(RollingOpsManager):
    """Class for controlling the locks in a retriable fashion.

    It differs from the main RollingOpsManager in two ways:
    1) It will take into account the OpenSearchOpsLock status before granting locks
    2) It will retry the lock acquisition if the restart-repeatable flag is set:
       that is used to indicate the unit requested the lock, but could not execute
       the operation because of a factor outside of its control. Use this resource
       whenever a given unit depends on the charm leader, for example, to progress.
    """

    def __init__(
        self,
        charm,
        relation: str,
        workload_lock_manager: WorkloadLockManager,
        callback: Any,
        acceptable_node_count_down: int = 0,
    ):
        """Constructor for the manager."""
        super().__init__(charm, relation, callback)
        self.workload_manager = workload_lock_manager
        # TODO: take the value below into consideration
        self.acceptable_node_count_down = acceptable_node_count_down

        # Given the process_locks may abandon relation-changed events because
        # the workload manager lock is being held, we must listen to more events.
        for event in [
            charm.on.update_status,
            charm.on.leader_elected,
            charm.on[self.name].relation_departed,
        ]:
            self.framework.observe(event, self._on_relation_changed)

        self.relation = charm.model.get_relation(self.name)

        # Calling this here guarantees we will check, for each node, if we
        # should reissue a lock request on every hook.
        if self.relation and self._should_lock_be_reacquired():
            callback = self.relation.data[self.charm.unit].get("callback_override", "")
            charm.on[self.name].acquire_lock.emit(
                callback_override=self.relation.data[self.charm.unit].update(
                    {"callback_override": callback}
                )
            )

    def _on_acquire_lock(self, event):
        """Acquires the lock. Restart the retry-lock counter."""
        RetriableLock(self, self.charm.unit).retrial_count = 0
        return super()._on_acquire_lock(event)

    def _should_lock_be_reacquired(self):
        """Checks if the restart should be retried now.

        For that, the unit has registered the restart-repeatable flag in the service
        relation data and the lock is not held or pending anymore.
        """
        lock = RetriableLock(self, self.charm.unit)
        return (
            # TODO: consider cases with a limitation in the amount of retries
            lock.retry()
            and not (lock.is_held() or lock.is_pending())
        )

    def _on_run_with_lock(self, event):
        """Method for running with lock."""
        lock = RetriableLock(self, self.charm.unit)
        try:
            super()._on_run_with_lock(event)
            if event.deferred:
                raise RollingOpsRetryLockLaterException
            return
        except RollingOpsRetryLockLaterException:
            logger.info("Retrying to acquire the lock later.")
            lock.retrial_count = lock.retrial_count + 1
        except Exception as e:
            logger.exception(f"Error while running with lock: {str(e)}")

        # A retriable error happened, raised by the callback method
        # It means the logic after callback execution was not ran.
        # Release the lock now, so we can reissue it later
        lock.release()  # Updates relation data

        # cleanup old callback overrides:
        # we do not clean up the callback override, so we can reissue it later
        # self.relation.data[self.charm.unit].update({"callback_override": ""})
        if self.model.unit.status.message == f"Executing {self.name} operation":
            self.model.unit.status = ActiveStatus()

    def _on_process_locks(self, event: EventBase):  # noqa: C901
        """Processes the locks.

        There are certain special rules to be considered when providing the lock:
        1) The node is trying to acquire it
        2) There is no node departing in the cluster
        3) There is no node with the service stopped in the cluster

        We build the lock following the original _on_process_locks scheme. Then,
        we should check the workload lock and ensure it is not held. If it is, we abandon
        the event and wait for the next peer relation departed to reprocess.

        We check that each node is reachable and healthy.
        If not and node is requesting lock, then the it is set to: ACQUIRE_BUT_SVC_NOT_RESPONSIVE

        Finally, if we have any of the following:
        1) Nodes helding this lock
        2) At least one node departing via workload lock
        3) Nodes with stopped service that did not try to acquire the lock
        We abandon the process and do not restart the cluster any further.
        """
        if not self.charm.unit.is_leader():
            return

        pending = []

        # First pass:
        # Find if we can process locks or should we wait for the next event.
        # Build a list of units that are pending.
        for lock in RetriableLocks(self):
            if lock.is_held():
                # One of our units has the lock -- return without further processing.
                return

            if lock.release_requested():
                lock.clear()  # Updates relation data

            if lock.is_blocked():
                return

            if lock.is_pending():
                if lock.unit == self.model.unit:
                    # Always run on the leader last.
                    pending.insert(0, lock)
                else:
                    pending.append(lock)

        # Find the next lock we want to process. We check for lock priority
        # 1) Do we have any locks with: ACQUIRE_BUT_SVC_NOT_RESPONSIVE
        # 2) Do we have any locks with: RETRY_LOCK
        # 3) Do we have any locks with: ACQUIRE (all the remaining)
        next_lock_to_process = None
        for lock in pending:
            # 1) Do we have any locks with: ACQUIRE_BUT_SVC_NOT_RESPONSIVE
            if lock.acquire_with_stopped_service():
                next_lock_to_process = lock
                break

            # 2) Do we have any locks with: RETRY_LOCK
            if lock.retry():
                next_lock_to_process = lock
                break

        if not next_lock_to_process and pending:
            # 3) Do we have any locks with: ACQUIRE (all the remaining)
            next_lock_to_process = pending[-1]

        # If we reach this point, and we have pending units, we want to grant a lock to
        # one of them.
        if next_lock_to_process:
            self.model.app.status = MaintenanceStatus("Beginning rolling {}".format(self.name))
            next_lock_to_process.grant()
            if next_lock_to_process.unit == self.model.unit:
                # It's time for the leader to run with lock.
                self.charm.on[self.name].run_with_lock.emit()
            return

        if self.model.app.status.message == f"Beginning rolling {self.name}":
            self.model.app.status = ActiveStatus()

