#!/usr/bin/env python3
import argparse
import subprocess
import time


#def dispatch(run_cmd: str, unit_name: str, charm_dir: str, event_name: str):
#    """
#    Dispatch a custom charm event using juju-run.
#    Mirrors the pattern used in Postgres observer.
#    """
#    dispatch_sub_cmd = f"JUJU_DISPATCH_PATH=hooks/{event_name} {charm_dir}/dispatch"
#    subprocess.run([run_cmd, "-u", unit_name, dispatch_sub_cmd])  # noqa: S603,S607

def dispatch(run_cmd: str, unit_name: str, charm_dir: str, event_name: str):

    dispatch_sub_cmd = f"JUJU_DISPATCH_PATH=hooks/{event_name} {charm_dir}/dispatch"
    res=subprocess.run([run_cmd, "-u", unit_name, dispatch_sub_cmd])  # noqa: S603,S607
    print(f"dispatch rc={res.returncode}\nstdout:\n{res.stdout}\nstderr:\n{res.stderr}", flush=True)
    res.check_returncode()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--acquire-delay", type=int, default=5)
    parser.add_argument("--run-cmd", required=True)
    parser.add_argument("--unit-name", required=True)
    parser.add_argument("--charm-dir", required=True)
    parser.add_argument("--event-name", required=True)
    args = parser.parse_args()

    # Simulate "wait for lock"
    time.sleep(args.acquire_delay)

    # Lock granted -> dispatch event
    dispatch(args.run_cmd, args.unit_name, args.charm_dir, args.event_name)

    # In the real version, the worker would keep running holding the lease.
    # For the demo, just exit.
    return


if __name__ == "__main__":
    main()
