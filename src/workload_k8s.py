#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Implementation of WorkloadBase for running Valkey on K8s."""

import collections
import logging
import signal
import threading
from typing import IO, BinaryIO, override

import ops
from charmlibs import pathops
from ops import pebble
from tenacity import retry, retry_if_result, stop_after_attempt, wait_fixed

from common.exceptions import (
    ValkeyServiceNotAliveError,
    ValkeyServicesCouldNotBeStoppedError,
    ValkeyServicesFailedToStartError,
    ValkeyWorkloadCommandError,
)
from core.base_workload import ProcessHandle, TLSPaths, WorkloadBase
from literals import (
    ACL_FILE,
    ARCHIVE_STORAGE_PATH,
    CHARM,
    CONFIG_FILE,
    DATA_STORAGE_PATH,
    LOG_STORAGE_PATH,
    SENTINEL_ACL_FILE,
    SENTINEL_CONFIG_FILE,
)

logger = logging.getLogger(__name__)


class _K8sProcessHandle:
    """ProcessHandle backed by Pebble exec (K8s substrate).

    stdout is streamed to the caller unbuffered. stderr is drained on a
    daemon thread into a bounded buffer (last 64 chunks) so a chatty child
    cannot fill the pipe and block -- ``wait()`` therefore returns only the
    tail of stderr. ``wait()`` returns the exit code WITHOUT calling
    ``wait_output()``: the latter would buffer the entire stdout (the whole
    RDB) into the charm container's memory. When Pebble itself fails to run
    the change, ``wait()`` returns -1 as the returncode sentinel.
    ``kill()`` sends SIGKILL but does not block waiting for the exit.
    """

    def __init__(self, process: pebble.ExecProcess):
        self._process = process
        if process.stdout is None:
            raise RuntimeError("Pebble exec must be created with stdout available")
        self.stdout: IO[bytes] = process.stdout
        self._stderr_buf: collections.deque[bytes] = collections.deque(maxlen=64)
        self._stderr_thread = threading.Thread(target=self._drain_stderr, daemon=True)
        self._stderr_thread.start()

    def _drain_stderr(self) -> None:
        """Drain the child's stderr into the bounded tail buffer until EOF.

        Runs on a daemon thread so a chatty child cannot fill the stderr
        pipe and block the caller draining stdout. Read failures are logged
        and terminate the thread rather than propagating to the caller.
        """
        stderr = self._process.stderr
        if stderr is None:
            return
        try:
            for chunk in iter(lambda: stderr.read(4096), b""):
                self._stderr_buf.append(chunk)
        except Exception:
            logger.exception("stderr drain thread failed")

    def wait(self) -> tuple[int, str]:
        try:
            self._process.wait()
            rc = 0
        except pebble.ExecError as e:
            rc = e.exit_code
        except pebble.ChangeError as e:
            logger.error("Pebble exec did not complete: %s", e)
            rc = -1
        except pebble.ConnectionError as e:
            logger.error("Pebble unreachable waiting for exec: %s", e)
            rc = -1
        self._stderr_thread.join(timeout=5)
        return rc, b"".join(self._stderr_buf).decode("utf-8", "replace")

    def kill(self) -> None:
        try:
            self._process.send_signal(signal.SIGKILL)
        except pebble.ConnectionError as e:
            # Pebble itself is unreachable -- the exec may still be running
            # and we have lost the ability to stop it. A real problem.
            logger.error("Cannot reach Pebble to SIGKILL the exec: %s", e)
        except pebble.Error as e:
            # Most often the exec has already finished, so there is nothing
            # to signal. kill() is called on cleanup paths after the process
            # may have exited on its own, so this is expected -- DEBUG, not
            # WARNING, to avoid alarming noise on the happy path.
            logger.debug("SIGKILL to Pebble exec was a no-op (likely already exited): %s", e)


class ValkeyK8sWorkload(WorkloadBase):
    """Implementation of WorkloadBase for running a Valkey container on K8s."""

    def __init__(self, container: ops.Container | None) -> None:
        if not container:
            raise AttributeError("Container is required.")

        self.container = container
        self.root_dir = pathops.ContainerPath("/", container=self.container)
        self.config_file = self.root_dir / CONFIG_FILE
        self.sentinel_config_file = self.root_dir / SENTINEL_CONFIG_FILE
        self.acl_file = self.root_dir / ACL_FILE
        self.sentinel_acl_file = self.root_dir / SENTINEL_ACL_FILE
        # todo: update this path once directories in the rock are complying with the standard
        self.working_dir = self.root_dir / DATA_STORAGE_PATH
        self.log_dir = self.root_dir / LOG_STORAGE_PATH
        self.archive_dir = self.root_dir / ARCHIVE_STORAGE_PATH
        self.lib_dir = self.root_dir / "usr/lib/valkey/modules"
        self.tls_dir = self.root_dir / "var/lib/valkey/tls"
        self.tls_paths: TLSPaths = TLSPaths(tls_root=self.tls_dir)
        self.valkey_service = "valkey"
        self.sentinel_service = "sentinel"
        self.metric_service = "metrics-exporter"
        self.cli = "valkey-cli"
        self.user = "_daemon_"

    @property
    @override
    def can_connect(self) -> bool:
        return self.container.can_connect()

    @property
    def pebble_layer(self) -> pebble.Layer:
        """Create the Pebble configuration layer for Valkey."""
        layer_config: pebble.LayerDict = {
            "summary": "Valkey layer",
            "description": "Valkey layer",
            "services": {
                self.valkey_service: {
                    "override": "replace",
                    "summary": "Valkey service",
                    "command": f"valkey-server {self.config_file.as_posix()}",
                    "user": self.user,
                    "group": self.user,
                    "startup": "enabled",
                },
                self.sentinel_service: {
                    "override": "replace",
                    "summary": "Valkey sentinel service",
                    "command": f"valkey-sentinel {self.sentinel_config_file.as_posix()}",
                    "user": self.user,
                    "group": self.user,
                    "startup": "enabled",
                },
                self.metric_service: {
                    "override": "replace",
                    "summary": "Valkey metric exporter",
                    "command": "prometheus-redis-exporter",
                    "user": self.user,
                    "group": self.user,
                    "startup": "enabled",
                },
            },
        }
        return pebble.Layer(layer_config)

    @override
    def start(self, service: str | None = None, check_alive: bool = True) -> None:
        try:
            if service:
                self.container.start(service)
            else:
                self.container.add_layer(CHARM, self.pebble_layer, combine=True)
                self.container.restart(
                    self.valkey_service, self.sentinel_service, self.metric_service
                )
        except (
            pebble.ChangeError,
            pebble.ConnectionError,
            pebble.APIError,
        ) as e:
            raise ValkeyServicesFailedToStartError(f"Failed to start Valkey services: {e}") from e
        if check_alive and not self.alive(service):
            raise ValkeyServiceNotAliveError("Valkey service is not alive after start.")

    @override
    def restart(self, service: str) -> None:
        try:
            self.container.restart(service)
        except (
            pebble.ChangeError,
            pebble.ConnectionError,
            pebble.APIError,
        ) as e:
            raise ValkeyServicesFailedToStartError(
                f"Failed to start service {service}: {e}"
            ) from e

    @override
    def push_data_file(
        self,
        src: BinaryIO,
        dest: pathops.PathProtocol,
        user: str | None = None,
        group: str | None = None,
    ) -> None:
        # K8s: separate containers; stream into the workload via Pebble push.
        try:
            self.container.push(
                dest.as_posix(),
                source=src,
                make_dirs=True,
                user=user,
                group=group or user,
            )
        except (pebble.PathError, pebble.ConnectionError, pebble.APIError) as e:
            raise ValkeyWorkloadCommandError(e)

    @override
    def move_file(self, src: pathops.PathProtocol, dest: pathops.PathProtocol) -> None:
        # No pathops rename inside the container; shell out via Pebble exec.
        try:
            self.container.exec(command=["mv", src.as_posix(), dest.as_posix()]).wait()
        except (
            pebble.ExecError,
            pebble.ChangeError,
            pebble.ConnectionError,
            pebble.APIError,
        ) as e:
            raise ValkeyWorkloadCommandError(e)

    @override
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(1),
        retry=retry_if_result(lambda healthy: not healthy),
        retry_error_callback=lambda _: False,
    )
    def alive(self, service: str | None = None) -> bool:
        services = (
            [service]
            if service
            else [self.valkey_service, self.sentinel_service, self.metric_service]
        )
        try:
            for service_name in services:
                if not self.container.get_service(service_name).is_running():
                    return False
        except (pebble.ConnectionError, ops.ModelError) as e:
            logger.warning("Cannot check service health: %s", e)
            return False
        return True

    @override
    def exec(
        self, command: list[str], env: dict[str, str] | None = None
    ) -> tuple[str, str | None]:
        try:
            process = self.container.exec(
                command=command,
                environment=env,
            )
            return process.wait_output()
        except pebble.APIError as e:
            logger.error("Command failed with %s, %s", e.code, e.body)
            raise ValkeyWorkloadCommandError(e)
        except pebble.ExecError as e:
            logger.error("Command failed with: %s, %s", e.exit_code, e.stdout)
            raise ValkeyWorkloadCommandError(e)
        except (pebble.ChangeError, pebble.ConnectionError) as e:
            logger.error("Command failed: %s", e)
            raise ValkeyWorkloadCommandError(e)

    @override
    def exec_stream(self, command: list[str], env: dict[str, str] | None = None) -> ProcessHandle:
        try:
            return _K8sProcessHandle(
                self.container.exec(command=command, encoding=None, timeout=None, environment=env)
            )
        except (pebble.ConnectionError, pebble.APIError, pebble.ExecError) as e:
            raise ValkeyWorkloadCommandError(e) from e

    @override
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(1),
        reraise=True,
    )
    def stop(self, service: str | None = None, check_alive: bool = False) -> None:
        targets = (
            (service,)
            if service
            else (self.valkey_service, self.sentinel_service, self.metric_service)
        )
        try:
            self.container.stop(*targets)
        except (
            pebble.ChangeError,
            pebble.TimeoutError,
            pebble.ConnectionError,
            pebble.APIError,
        ) as e:
            logger.error("Failed to stop Valkey services: %s", e)
            raise ValkeyServicesCouldNotBeStoppedError(
                f"Failed to stop Valkey services: {e}"
            ) from e
        # Opt-in: verify the stopped service(s) went down.
        if check_alive and self.alive(service):
            raise ValkeyServicesCouldNotBeStoppedError(
                f"Service {service or 'valkey'} still running after stop."
            )

    @override
    def total_memory_bytes(self) -> int:
        """Pod's cgroup v2 memory limit read from inside the Valkey container.

        Falls back to /proc/meminfo MemTotal when the cgroup limit is unset
        ("max"). Returns 0 if neither is readable. Cgroup v1 is not supported
        (Ubuntu noble — the only supported substrate — is cgroup v2 only).
        """
        return self._read_cgroup_limit("sys/fs/cgroup/memory.max") or self._read_meminfo_total()
