#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for Cluster Topology."""

import logging
import os
import signal
import subprocess
from pathlib import Path
from sys import version_info

from core.base_workload import WorkloadBase
from core.cluster_state import ClusterState
from literals import (
    SENTINEL_PORT,
    SENTINEL_TLS_PORT,
    TOPOLOGY_OBSERVER_LOG_FILENAME,
    TOPOLOGY_OBSERVER_PID_FILENAME,
    TOPOLOGY_OBSERVER_TLS_CA_FILENAME,
    CharmUsers,
)

logger = logging.getLogger(__name__)


class TopologyManager:
    """Observe the topology for Valkey Sentinel."""

    name: str = "topology_observer"
    state: ClusterState

    def __init__(self, state: ClusterState, workload: WorkloadBase):
        self.state = state
        self.workload = workload

    @property
    def _log_file_path(self) -> Path:
        """Return the path to the topology observer log file."""
        return self.state.charm.charm_dir / TOPOLOGY_OBSERVER_LOG_FILENAME

    @property
    def _tls_ca_file_path(self) -> Path:
        """Return the path to the topology observer TLS CA file."""
        return self.state.charm.charm_dir / TOPOLOGY_OBSERVER_TLS_CA_FILENAME

    @property
    def _pid_file_path(self) -> Path:
        """Return the path to the topology observer pid file."""
        return self.state.charm.charm_dir / TOPOLOGY_OBSERVER_PID_FILENAME

    def start_observer(self) -> None:
        """Start the topology observer as a subprocess."""
        if (observer_pid := self._read_observer_pid()) != 0:
            try:
                # check if the process already runs
                os.kill(int(observer_pid), 0)
                return
            except OSError:
                logger.debug("Topology observer not running")
                pass

        # Generate the venv path based on the existing lib path
        env = os.environ.copy()
        env.pop("JUJU_CONTEXT_ID", None)
        for loc in env["PYTHONPATH"].split(":"):
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
                env["PYTHONPATH"] = f"{venv_path.resolve()}:{env['PYTHONPATH']}"
                break

        # Gather Valkey hosts for connection
        started_servers = [
            unit.get_endpoint(self.state.substrate)
            for unit in self.state.servers
            if unit.is_active
        ]
        port = SENTINEL_TLS_PORT if self.state.unit_server.is_tls_enabled else SENTINEL_PORT
        hosts = ",".join(sorted([f"{server}:{port}" for server in started_servers]))

        if self.state.unit_server.is_tls_enabled:
            # Store current TLS CA cert on operator container
            tls_ca_cert = self.workload.read_file(self.workload.tls_paths.client_ca)
            self._tls_ca_file_path.write_text(tls_ca_cert)

        logging.info("Starting topology observer")
        pid = subprocess.Popen(  # noqa: S603
            [
                "/usr/bin/python3",
                "src/scripts/topology_observer.py",
                hosts,
                CharmUsers.SENTINEL_CHARM_ADMIN.value,  # username
                self.state.cluster.internal_users_credentials.get(
                    CharmUsers.SENTINEL_CHARM_ADMIN.value, ""
                ),  # password
                str(self.state.unit_server.is_tls_enabled),
                self.state.unit_server.unit_name,
                self.state.charm.charm_dir,
            ],
            # File shouldn't close
            stdout=open(self._log_file_path.as_posix(), "a"),  # noqa: SIM115
            stderr=subprocess.STDOUT,
            env=env,
        ).pid

        self._pid_file_path.write_text(str(pid))
        logging.info(f"Started topology observer process with PID {pid}")

    def stop_observer(self) -> None:
        """Stop the topology observer."""
        if (observer_pid := self._read_observer_pid()) == 0:
            logger.debug("Topology observer already stopped")
            return

        logger.debug("Stopping topology observer")
        try:
            os.kill(int(observer_pid), signal.SIGTERM)
            logger.info("Topology observer stopped")
        except OSError:
            pass
        finally:
            self._pid_file_path.unlink()

    def restart_observer(self) -> None:
        """Stop and start the topology observer to pickup host changes."""
        self.stop_observer()
        self.start_observer()

    def _read_observer_pid(self) -> int:
        """Read the pid file of the topology observer and return the pid, or 0 if none."""
        try:
            return int(self._pid_file_path.read_text())
        except (FileNotFoundError, PermissionError):
            return 0
