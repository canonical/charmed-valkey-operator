#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Fixtures for object-storage backup tests: S3 (MicroCeph), Azure (Azurite), GCS (real).

MicroCeph's RGW is fronted with a self-signed TLS certificate generated here,
so the suite exercises the charm's full S3-over-TLS path (CA-chain
distribution + boto3 verification), not just plaintext S3. The certificate's
SAN covers the host's routable IP because the charm units (k8s pods / lxd
machines) reach the gateway over that address -- never loopback, which from a
unit resolves to the unit itself.

Every MicroCeph step is idempotent so the suite can be re-run locally without
tearing the cluster down between runs.
"""

from __future__ import annotations

import base64
import json
import os
import secrets
import socket
import subprocess
import time
from pathlib import Path

import boto3
import pytest
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import ContainerClient
from botocore.config import Config
from google.api_core.exceptions import NotFound
from google.cloud import storage

from literals import Substrate

RGW_SSL_PORT = 445
# Self-signed cert + key, persisted so repeated local runs reuse the exact
# material the already-running gateway serves -- regenerating without
# re-enabling RGW would break TLS verification against the old certificate.
# Named for the MicroCeph RGW (not this product) so a shared MicroCeph serving
# several data-platform suites reuses one gateway certificate.
_CERT_DIR = Path.home() / ".cache" / "microceph-rgw"
_CERT = _CERT_DIR / "rgw-cert.pem"
_KEY = _CERT_DIR / "rgw-key.pem"


def _run(*cmd: str, **kwargs) -> str:
    """Run a command, returning its stdout; raise CalledProcessError (with stderr) on failure."""
    return subprocess.run(cmd, capture_output=True, text=True, check=True, **kwargs).stdout.strip()


def _host_ip() -> str:
    """First routable IPv4 of this host, reachable from charm units."""
    return _run("hostname", "-I").split()[0]


def _port_open(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(1)
        return sock.connect_ex((host, port)) == 0


def _ensure_microceph() -> None:
    """Install + bootstrap MicroCeph with OSDs; each step idempotent."""
    try:
        _run("snap", "list", "microceph")
    except subprocess.CalledProcessError:
        _run("sudo", "snap", "install", "microceph", "--channel=squid/stable")
    try:
        _run("sudo", "microceph", "status")
    except subprocess.CalledProcessError:
        _run("sudo", "microceph", "cluster", "bootstrap")
        _run("sudo", "microceph", "disk", "add", "loop,4G,3", "--wipe")


def _ensure_rgw_tls(host_ip: str) -> str:
    """Serve RGW over TLS on RGW_SSL_PORT with a SAN-correct self-signed cert.

    Returns the certificate PEM. It is self-signed, so it is also the CA chain
    handed to s3-integrator.
    """
    have_cert = _CERT.exists() and _KEY.exists()
    if not have_cert:
        _CERT_DIR.mkdir(parents=True, exist_ok=True)
        _run(
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
            "-keyout", _KEY.as_posix(), "-out", _CERT.as_posix(),
            "-days", "3650", "-subj", "/CN=valkey-microceph-rgw",
            "-addext", f"subjectAltName=IP:{host_ip},IP:127.0.0.1,DNS:localhost",
        )  # fmt: skip
    # (Re)configure RGW when the cert is freshly minted or the gateway is down:
    # a new certificate must replace whatever the running gateway presents.
    if not have_cert or not _port_open(host_ip, RGW_SSL_PORT):
        try:
            _run("sudo", "microceph", "disable", "rgw")
        except subprocess.CalledProcessError:
            pass  # already disabled -- nothing to tear down before re-enabling
        _run(
            "sudo", "microceph", "enable", "rgw",
            f"--ssl-port={RGW_SSL_PORT}",
            f"--ssl-certificate={base64.b64encode(_CERT.read_bytes()).decode()}",
            f"--ssl-private-key={base64.b64encode(_KEY.read_bytes()).decode()}",
        )  # fmt: skip
    for _ in range(30):
        if _port_open(host_ip, RGW_SSL_PORT):
            break
        time.sleep(1)
    return _CERT.read_text()


def _ensure_user(uid: str = "test") -> tuple[str, str]:
    """Get-or-create an RGW user; return (access_key, secret_key)."""
    try:
        out = _run("sudo", "radosgw-admin", "user", "info", f"--uid={uid}")
    except subprocess.CalledProcessError:
        out = _run(
            "sudo", "radosgw-admin", "user", "create",
            f"--uid={uid}", f"--display-name={uid}",
        )  # fmt: skip
    keys = json.loads(out)["keys"][0]
    return keys["access_key"], keys["secret_key"]


@pytest.fixture(scope="module")
def microceph() -> dict:
    """Install MicroCeph, serve TLS RGW, and return its S3 connection params."""
    _ensure_microceph()
    host_ip = _host_ip()
    cert = _ensure_rgw_tls(host_ip)
    access_key, secret_key = _ensure_user()

    return {
        "endpoint": f"https://{host_ip}:{RGW_SSL_PORT}",
        "access-key": access_key,
        "secret-key": secret_key,
        "bucket": f"valkey-backup-{secrets.token_hex(4)}",
        "region": "default",
        "path": "valkey",
        "tls-ca-chain": [cert],
    }


@pytest.fixture(scope="module")
def s3_bucket(microceph):
    """Return a boto3 Bucket resource for the test bucket, creating it eagerly."""
    s3 = boto3.resource(
        "s3",
        region_name=microceph["region"],
        endpoint_url=microceph["endpoint"],
        aws_access_key_id=microceph["access-key"],
        aws_secret_access_key=microceph["secret-key"],
        config=Config(
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
        verify=False,
    )
    bucket = s3.Bucket(microceph["bucket"])
    bucket.create()
    bucket.wait_until_exists()
    return bucket


# ── Azurite (Azure Blob emulator) ─────────────────────────────────────────────
# A host service, like MicroCeph above, reached from both substrates at the
# host's routable IP. The snap bundles its own Node runtime and runs the blob
# endpoint as a daemon, so no container runtime is involved -- the integration
# workflow purges Docker so Canonical K8s can install over a clean containerd.
AZURITE_PORT = 10000
AZURITE_SERVICE = "azurite.blob"
# The only channel carrying a revision today; stable/candidate/beta are empty.
# Move to a stable track once the snap is promoted -- edge moves under the suite
# the way an unpinned image tag would.
_AZURITE_CHANNEL = "latest/edge"
# Azurite's well-known development account name + key. Published in Microsoft's
# emulator docs and hardcoded in Azurite itself -- public test material, not a
# secret, and it only ever authenticates against the local emulator.
_AZURITE_ACCOUNT = "devstoreaccount1"
_AZURITE_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)


def _wait_for_port(host: str, port: int, timeout: int = 60) -> bool:
    for _ in range(timeout):
        if _port_open(host, port):
            return True
        time.sleep(1)
    return False


def _ensure_azurite(host_ip: str) -> int:
    """Install + serve Azurite's blob endpoint on the host; each step idempotent.

    Returns the port, or 0 when it could not be served -- no snapd, no network --
    which the fixture turns into a skip.
    """
    try:
        try:
            _run("snap", "list", "azurite")
        except subprocess.CalledProcessError:
            _run("sudo", "snap", "install", "azurite", f"--channel={_AZURITE_CHANNEL}")
        # Out of the box the daemon binds 127.0.0.1, which from a unit resolves to
        # the unit itself; charm units reach the emulator at the host's routable IP.
        _run("sudo", "snap", "set", "azurite", "host=0.0.0.0")
        # The snap ships install-mode: disable, so the daemon needs an explicit
        # start; --enable is a no-op on a running service.
        _run("sudo", "snap", "start", "--enable", AZURITE_SERVICE)
        # `snap set` does not bounce the daemon and the launcher reads `host` only
        # at start, so the restart is what actually applies the bind address. It
        # also starts a stopped service, making this safe from any prior state.
        _run("sudo", "snap", "restart", AZURITE_SERVICE)
    except (FileNotFoundError, subprocess.CalledProcessError):
        return 0
    return AZURITE_PORT if _wait_for_port(host_ip, AZURITE_PORT) else 0


@pytest.fixture(scope="module")
def azurite() -> dict:
    """Start Azurite and return the azure_storage envelope pointing at it.

    Reachable from charm units at the host's routable IP -- never loopback,
    which from a unit resolves to the unit itself (same rationale as the
    MicroCeph fixture). `endpoint` embeds the dev account in the path and
    `connection-protocol` is plain `http`, which azure-storage-integrator
    accepts for Blob REST access, so it talks to the emulator, not real Azure.
    """
    host_ip = _host_ip()
    if not (port := _ensure_azurite(host_ip)):
        pytest.skip("Azurite unavailable: could not install or start it on this host")

    return {
        "container": f"valkey-backup-{secrets.token_hex(4)}",
        "storage-account": _AZURITE_ACCOUNT,
        "secret-key": _AZURITE_KEY,
        "connection-protocol": "http",
        "endpoint": f"http://{host_ip}:{port}/{_AZURITE_ACCOUNT}",
        "path": "valkey",
    }


@pytest.fixture(scope="module")
def azure_container(azurite: dict):
    """Return an Azure ContainerClient for the test container, creating it eagerly.

    Mirrors `s3_bucket`, and built exactly the way the charm's AzureBackend builds
    its client (account_url = endpoint, account named explicitly), so the test
    inspects the very store the charm writes to.
    """
    container = ContainerClient(
        account_url=azurite["endpoint"],
        container_name=azurite["container"],
        credential={
            "account_name": azurite["storage-account"],
            "account_key": azurite["secret-key"],
        },
    )
    try:
        container.create_container()
    except ResourceExistsError:
        pass  # re-run against an already-created container
    return container


# ── Google Cloud Storage (real) ───────────────────────────────────────────────
# Not an emulator: gcs-integrator publishes no endpoint, so the charm can only
# ever talk to storage.googleapis.com. Objects land under a per-run prefix in
# the shared test bucket and are cleaned up on teardown.
GCS_SERVICE_ACCOUNT_ENV = "GCS_SERVICE_ACCOUNT"
GCS_BUCKET_ENV = "GCS_BUCKET"
GCS_DEFAULT_BUCKET = "data-charms-testing"


def _service_account_info(value: str) -> dict:
    """Decode the service-account key the way the charm does.

    The env value is handed to the integrator verbatim (JSON or base64 JSON) and
    only decoded here, for the test's own client. Inner whitespace is dropped
    first: `base64` without -w0 wraps at 76 columns, and spread's env export
    turns those newlines into spaces.
    """
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        packed = "".join(value.split())
        return json.loads(base64.b64decode(packed, altchars=b"-_", validate=True))


@pytest.fixture(scope="module")
def gcs(substrate: Substrate) -> dict:
    """Return the gcs envelope: a per-run prefix in the shared test bucket.

    A missing or empty key is a hard failure, never a skip: a silently skipped
    module looks green in CI forever. Empty, not just missing, because an
    ungranted GitHub secret expands to "" through `secrets: inherit`.
    """
    value = os.environ.get(GCS_SERVICE_ACCOUNT_ENV, "")
    if not value:
        pytest.fail(
            f"{GCS_SERVICE_ACCOUNT_ENV} is unset or empty; the GCS module needs a "
            "service-account key (JSON or base64 JSON) and never skips"
        )
    return {
        "bucket": os.environ.get(GCS_BUCKET_ENV, GCS_DEFAULT_BUCKET),
        "path": f"valkey-{substrate}-{secrets.token_hex(4)}",
        "secret-key": value,
    }


@pytest.fixture(scope="module")
def gcs_bucket(gcs: dict):
    """Return a google-cloud-storage Bucket for the test bucket; clean the prefix after.

    Built the way the charm's GCSBackend builds its client (service-account
    info, explicit project), so the test inspects the very store the charm
    writes to. Teardown deletes every object under this run's prefix so the
    shared bucket does not accumulate RDB snapshots (a run killed by the CI step
    timeout skips this; an object-lifecycle rule on the bucket is the backstop).
    """
    info = _service_account_info(gcs["secret-key"])
    client = storage.Client.from_service_account_info(info, project=info.get("project_id"))
    bucket = client.bucket(gcs["bucket"])
    yield bucket
    for blob in client.list_blobs(gcs["bucket"], prefix=f"{gcs['path']}/"):
        try:
            blob.delete()
        except NotFound:
            pass  # a concurrent cleanup got there first; the goal is an empty prefix
