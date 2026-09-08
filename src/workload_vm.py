#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Implementation of WorkloadBase for running Valkey on VMs."""

import collections
import logging
import os
import platform
import shutil
import subprocess
import threading
import time
from typing import BinaryIO, override

from charmlibs import pathops, snap
from tenacity import (
    Retrying,
    retry,
    retry_if_exception_type,
    retry_if_result,
    stop_after_attempt,
    wait_fixed,
)

from common.exceptions import (
    ValkeyServiceNotAliveError,
    ValkeyServicesCouldNotBeStoppedError,
    ValkeyServicesFailedToStartError,
    ValkeyWorkloadCommandError,
)
from core.base_workload import ProcessHandle, TLSPaths, WorkloadBase
from literals import (
    ARCHIVE_STORAGE_PATH,
    DATA_STORAGE_PATH,
    LOG_STORAGE_PATH,
    SNAP_ACL_FILE,
    SNAP_COMMON_PATH,
    SNAP_CONFIG_FILE,
    SNAP_CURRENT_PATH,
    SNAP_NAME,
    SNAP_REVISIONS,
    SNAP_SENTINEL_ACL_FILE,
    SNAP_SENTINEL_CONFIG_FILE,
    SNAP_SENTINEL_SERVICE,
    SNAP_SERVICE,
)

logger = logging.getLogger(__name__)


class _VmProcessHandle:
    """ProcessHandle backed by ``subprocess.Popen`` (VM substrate).

    stdout is the Popen pipe, streamed to the caller unbuffered. stderr is
    drained on a daemon thread so a chatty child cannot fill the pipe and
    deadlock the upload. ``wait()`` returns the real process exit code and
    reaps the child; ``kill()`` sends SIGKILL and waits briefly so the
    process does not linger as a zombie.
    """

    def __init__(self, proc: subprocess.Popen):
        self._proc = proc
        if proc.stdout is None:  # asserts compile away under -O
            raise RuntimeError("subprocess.Popen must be created with stdout=PIPE")
        self.stdout = proc.stdout
        # Bounded: a chatty child must not grow this without limit over a
        # long-running stream. Only the tail is kept, matching the K8s handle.
        self._stderr_buf: collections.deque[bytes] = collections.deque(maxlen=64)
        self._stderr_thread = threading.Thread(target=self._drain_stderr, daemon=True)
        self._stderr_thread.start()

    def _drain_stderr(self) -> None:
        """Drain the child's stderr into the bounded tail buffer until EOF.

        Runs on a daemon thread so a chatty child cannot fill the stderr
        pipe and block the caller draining stdout. Read failures are logged
        and terminate the thread rather than propagating to the caller.
        """
        stderr = self._proc.stderr
        if stderr is None:
            return
        try:
            for chunk in iter(lambda: stderr.read(4096), b""):
                self._stderr_buf.append(chunk)
        except Exception:
            logger.exception("stderr drain thread failed")

    def wait(self) -> tuple[int, str]:
        rc = self._proc.wait()
        # The process has exited; closing stderr unblocks the drain thread's
        # blocking read() so the join() below cannot hang.
        if self._proc.stderr is not None:
            self._proc.stderr.close()
        self._stderr_thread.join()
        return rc, b"".join(self._stderr_buf).decode("utf-8", "replace")

    def kill(self) -> None:
        self._proc.kill()
        try:
            self._proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            logger.warning("Process did not exit within 10s of SIGKILL")


class ValkeyVmWorkload(WorkloadBase):
    """Implementation of WorkloadBase for running a Valkey container on VM."""

    def __init__(self) -> None:
        for attempt in Retrying(stop=stop_after_attempt(5), wait=wait_fixed(5)):
            with attempt:
                self.valkey = snap.SnapCache()[SNAP_NAME]

        self.root_dir = pathops.LocalPath("/")
        self.config_file = self.root_dir / SNAP_CURRENT_PATH / SNAP_CONFIG_FILE
        self.sentinel_config_file = self.root_dir / SNAP_CURRENT_PATH / SNAP_SENTINEL_CONFIG_FILE
        self.acl_file = self.root_dir / SNAP_CURRENT_PATH / SNAP_ACL_FILE
        self.sentinel_acl_file = self.root_dir / SNAP_CURRENT_PATH / SNAP_SENTINEL_ACL_FILE
        self.working_dir = self.root_dir / SNAP_COMMON_PATH / DATA_STORAGE_PATH
        self.log_dir = self.root_dir / SNAP_COMMON_PATH / LOG_STORAGE_PATH
        self.archive_dir = self.root_dir / SNAP_COMMON_PATH / ARCHIVE_STORAGE_PATH
        self.lib_dir = self.root_dir / f"snap/{SNAP_NAME}/current/usr/lib/valkey/modules"
        self.tls_dir = self.root_dir / SNAP_CURRENT_PATH / "tls"
        self.tls_paths: TLSPaths = TLSPaths(tls_root=self.tls_dir)
        self.valkey_service = SNAP_SERVICE
        self.sentinel_service = SNAP_SENTINEL_SERVICE
        self.cli = f"{SNAP_NAME}.cli"
        self.user = "snap_daemon"

    @property
    @override
    def can_connect(self) -> bool:
        try:
            return bool(self.valkey.services[self.valkey_service])
        except KeyError:
            return False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(5),
        reraise=True,
        retry=retry_if_exception_type(RuntimeError),
    )
    def install(self, revision: str | None = None, retry_and_raise: bool = True) -> bool:
        """Install the valkey snap from the snap store.

        Args:
            revision (str | None): the snap revision to install
            retry_and_raise (bool): whether to retry in case of errors. Will raise if the error
                persists.

        Returns:
            True if successfully installed, False if errors occur and `retry_and_raise` is False.
        """
        if not revision:
            revision = str(SNAP_REVISIONS[platform.machine()])

        try:
            self.valkey.ensure(snap.SnapState.Present, revision=revision)
            self.valkey.hold()
            return True
        except snap.SnapError as e:
            logger.error(str(e))
            if retry_and_raise:
                raise RuntimeError
            return False

    @override
    def start(self, service: str | None = None, check_alive: bool = True) -> None:
        services = [service] if service else [self.valkey_service, self.sentinel_service]
        try:
            self.valkey.start(services=services)
        except snap.SnapError as e:
            logger.exception(str(e))
            raise ValkeyServicesFailedToStartError(f"Failed to start Valkey services: {e}") from e

        # A service can start then die immediately; when check_alive, poll the
        # whole set (all-services start) or check the one service, matching k8s.
        if check_alive:
            alive = (
                self.alive(service) if service else self.wait_for_services_to_be_alive(duration=3)
            )
            if not alive:
                logger.error("Valkey service is not alive after start.")
                raise ValkeyServiceNotAliveError("Valkey service is not alive after start.")

    @override
    def restart(self, service: str) -> None:
        try:
            self.valkey.restart(services=[service])
        except snap.SnapError as e:
            logger.exception(str(e))
            raise ValkeyServicesFailedToStartError(
                "Failed to restart service %s: %s", service, e
            ) from e

    @override
    def push_data_file(
        self,
        src: BinaryIO,
        dest: pathops.PathProtocol,
        user: str | None = None,
        group: str | None = None,
    ) -> None:
        # VM: charm and snap share the filesystem; stream straight to disk, then chown.
        try:
            with open(dest.as_posix(), "wb") as fh:
                shutil.copyfileobj(src, fh)
            if user:
                shutil.chown(dest.as_posix(), user=user, group=group or user)
        except OSError as e:
            raise ValkeyWorkloadCommandError(e)

    @override
    def move_file(self, src: pathops.PathProtocol, dest: pathops.PathProtocol) -> None:
        # shutil.move (not os.replace): restore crosses the data<->archive
        # partitions (cross-device), which os.replace rejects with EXDEV.
        try:
            shutil.move(src.as_posix(), dest.as_posix())
        except OSError as e:
            raise ValkeyWorkloadCommandError(e)

    @override
    def exec(
        self, command: list[str], env: dict[str, str] | None = None
    ) -> tuple[str, str | None]:
        try:
            output = subprocess.run(
                command,
                check=True,
                text=True,
                capture_output=True,
                timeout=10,
                env={**os.environ, **env} if env else os.environ,
            )
            return output.stdout, output.stderr
        except subprocess.CalledProcessError as e:
            logger.error("Command failed with %s, %s", e.returncode, e.stderr)
            raise ValkeyWorkloadCommandError(e)
        except subprocess.TimeoutExpired as e:
            logger.error("Command timed out: %s", str(e.stderr))
            raise ValkeyWorkloadCommandError(e)

    @override
    def exec_stream(self, command: list[str], env: dict[str, str] | None = None) -> ProcessHandle:
        return _VmProcessHandle(
            subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=0,
                env={**os.environ, **env} if env else os.environ,
            )
        )

    @override
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(1),
        retry=retry_if_result(lambda healthy: not healthy),
        retry_error_callback=lambda _: False,
    )
    def alive(self, service: str | None = None) -> bool:
        services = [service] if service else [self.valkey_service, self.sentinel_service]
        try:
            return all(bool(self.valkey.services[s]["active"]) for s in services)
        except KeyError:
            return False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(1),
        retry=retry_if_result(lambda healthy: not healthy),
        retry_error_callback=lambda _: False,
    )
    def wait_for_services_to_be_alive(self, duration: float = 30, delay: float = 0.1) -> bool:
        """Poll until the Valkey services are alive for at least `duration` seconds.

        Args:
            duration (float): The maximum duration to poll for the services to be alive. Default is 30 seconds.
            delay (float): The delay between each poll attempt in seconds. Default is 0.1 seconds.

        Returns:
                bool: True if the services are alive within the poll duration, False otherwise.
        """
        deadline = time.time() + duration
        while time.time() < deadline:
            if not self.alive():
                return False

            time.sleep(delay)
        return True

    @override
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(1),
        reraise=True,
    )
    def stop(self, service: str | None = None, check_alive: bool = False) -> None:
        services = [service] if service else [SNAP_SERVICE, SNAP_SENTINEL_SERVICE]
        try:
            self.valkey.stop(services=services)
        except snap.SnapError as e:
            logger.error("Failed to stop Valkey services: %s", e)
            raise ValkeyServicesCouldNotBeStoppedError(
                f"Failed to stop Valkey services: {e}"
            ) from e

        # Opt-in: verify the stopped service(s) went down.
        if check_alive and self.alive(service):
            logger.error("Valkey service(s) still alive after stop.")
            raise ValkeyServicesCouldNotBeStoppedError("Valkey service(s) still alive after stop.")

    @override
    def total_memory_bytes(self) -> int:
        """Per-unit memory budget for this machine.

        Returns the cgroup v2 memory limit when one is set — an LXD
        ``limits.memory`` constraint maps to ``memory.max`` (or ``memory.high``
        in soft-enforce mode) — otherwise falls back to ``/proc/meminfo``
        MemTotal. lxcfs virtualizes MemTotal to the container limit inside an
        LXD system container, and it reflects the guest's RAM in an LXD VM,
        MAAS, or cloud VM. Returns 0 if nothing is readable. Cgroup v1 is not
        supported (Ubuntu noble is cgroup v2 only).
        """
        return (
            self._read_cgroup_limit("sys/fs/cgroup/memory.max")
            or self._read_cgroup_limit("sys/fs/cgroup/memory.high")
            or self._read_meminfo_total()
        )
