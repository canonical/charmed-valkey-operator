#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""High availability helpers."""

import base64
import json
import os
import string
import subprocess
import tempfile
import time
from datetime import datetime
from logging import getLogger

import jubilant
import urllib3
import yaml
from kubernetes import client, config, stream
from kubernetes.client.rest import ApiException
from tenacity import RetryError, Retrying, retry, stop_after_attempt, stop_after_delay, wait_fixed

from literals import Substrate
from tests.integration.helpers import APP_NAME, are_apps_active_and_agents_idle, get_sentinels

logger = getLogger(__name__)

VALKEY_SNAP_SERVICE_NAME = "snap.valkey-charmed.server.service"
VM_RESTART_DELAY_DEFAULT = 20
K8S_RESTART_DELAY_DEFAULT = 5
RESTART_DELAY_PATCHED = 120

# Budget for a whole `juju ssh` round trip (CLI start-up, controller API, SSH handshake,
# `sudo -i` on VM); the remote commands themselves are instantaneous, so this is a hang guard.
SSH_COMMAND_TIMEOUT = 60


EXTEND_PEBBLE_RESTART_DELAY_YAML = """services:
  valkey:
    override: merge
    backoff-delay: {delay}s
    backoff-limit: {delay}s
"""

RESTORE_PEBBLE_RESTART_DELAY_YAML = """services:
  valkey:
    override: merge
    backoff-delay: 500ms
    backoff-limit: 30s
"""

# Cut the network without changing the unit's IP by dropping packets inside the container:
# masking the NIC (the ip_change path) drops the DHCP lease, and a bandwidth throttle (what
# this used to do) still lets Sentinel PING/PONG trickle through, so +odown never reaches
# quorum and no failover starts. Loopback stays open for the charm's own health checks;
# `lxc exec` rides the LXD socket, so cut and restore work while the container is isolated.
NETWORK_CUT_RULES = (
    "INPUT ! --in-interface lo --jump DROP",
    "OUTPUT ! --out-interface lo --jump DROP",
)


def lxd_cut_network_from_unit_with_ip_change(machine_name: str) -> None:
    """Cut network from a lxc container in a way the changes the IP."""
    # apply a mask (device type `none`)
    cut_network_command = f"lxc config device add {machine_name} eth0 none"
    subprocess.check_call(cut_network_command.split())

    time.sleep(5)


def _lxd_delete_iptables_rule(machine_name: str, rule: str) -> bool:
    """Delete an iptables rule inside an lxc container, reporting whether it was there."""
    command = f"lxc exec {machine_name} -- iptables --delete {rule}"
    returncode = subprocess.call(
        command.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    return returncode == 0


def lxd_cut_network_from_unit_without_ip_change(machine_name: str) -> None:
    """Cut network from a lxc container (without causing the change of the unit IP address)."""
    for rule in NETWORK_CUT_RULES:
        # drop a leftover copy first so re-cutting cannot stack a duplicate rule
        _lxd_delete_iptables_rule(machine_name, rule)
        subprocess.check_call(f"lxc exec {machine_name} -- iptables --insert {rule}".split())


def lxd_restore_network_to_unit_without_ip_change(machine_name: str) -> None:
    """Restore the network of a lxc container that was cut without an IP address change."""
    for rule in NETWORK_CUT_RULES:
        # tolerate a missing rule so restoring twice is a no-op
        if not _lxd_delete_iptables_rule(machine_name, rule):
            logger.warning("iptables rule %r was not present on %s", rule, machine_name)


def k8s_cut_network_from_unit_without_ip_change(model_name: str, machine_name: str) -> None:
    """Cut network from a k8s pod without causing the change of the unit IP address."""
    # Apply a NetworkChaos file to use chaos-mesh to simulate a network cut.
    with tempfile.NamedTemporaryFile(dir=".") as temp_file:
        # Generates a manifest for chaosmesh to simulate network failure for a pod
        with open(
            "tests/integration/ha/helpers/chaos_network_loss.yml"
        ) as chaos_network_loss_file:
            logger.info(
                f"Calling network loss on ns={model_name} and pod={machine_name.replace('/', '-')}"
            )
            template = string.Template(chaos_network_loss_file.read())
            chaos_network_loss = template.substitute(
                namespace=model_name,
                pod=machine_name.replace("/", "-"),
            )

            temp_file.write(str.encode(chaos_network_loss))
            temp_file.flush()

        # Apply the generated manifest, chaosmesh would then make the pod inaccessible.
        # The chaos-mesh admission webhook (mnetworkchaos.kb.io) can take a while to
        # start serving after the controller-manager is installed; until it does the
        # apply fails with a transient "connection refused" calling the webhook. Retry
        # for a bounded window so the webhook has time to come up.
        env = os.environ
        env["KUBECONFIG"] = os.path.expanduser("~/.kube/config")
        command_result = None
        for attempt in Retrying(stop=stop_after_delay(120), wait=wait_fixed(5), reraise=True):
            with attempt:
                try:
                    command_result = subprocess.check_output(
                        " ".join(["sudo", "k8s", "kubectl", "apply", "-f", temp_file.name]),
                        shell=True,
                        env=env,
                        stderr=subprocess.STDOUT,
                    )
                except subprocess.CalledProcessError as err:
                    logger.error(
                        f"Failed to apply network isolation: [{err.returncode}] {err.stderr=}, {err.stdout=}"
                    )
                    raise
        logger.info("Result of isolating unit from cluster is '%s'", command_result)

        # `kubectl apply` only means the API server accepted the NetworkChaos; chaos-mesh
        # programs the netem rule asynchronously (NetworkChaos -> PodNetworkChaos ->
        # chaos-daemon), which can take longer than the reachability probe's pod start-up.
        # Wait until chaos-mesh reports the rule injected before returning, so the "cut" is
        # synchronous like the LXD iptables variant.
        _k8s_wait_network_chaos_injected(model_name)


def _k8s_wait_network_chaos_injected(namespace: str) -> None:
    """Block until the `network-loss-primary` NetworkChaos is selected and fully injected.

    Chaos-mesh only flips a record to `Injected` after the daemon applied the tc rule
    (PodNetworkChaos `observedGeneration` catches up), so `AllInjected=True` is the real
    "packets are being dropped" signal. `Selected=True` is required as well: before any pod
    is selected the record loop is empty and `AllInjected` is vacuously True.
    """
    for attempt in Retrying(stop=stop_after_delay(120), wait=wait_fixed(2), reraise=True):
        with attempt:
            output = subprocess.check_output(
                f"sudo k8s kubectl -n {namespace} get networkchaos network-loss-primary -o json",
                shell=True,
                env=os.environ,
                stderr=subprocess.STDOUT,
            )
            conditions = {
                cond["type"]: cond["status"]
                for cond in json.loads(output).get("status", {}).get("conditions", [])
            }
            if conditions.get("Selected") != "True" or conditions.get("AllInjected") != "True":
                raise ValueError(f"network chaos not injected yet: {conditions}")
            logger.info("Network chaos injected: %s", conditions)


def cut_network_from_unit(
    substrate: Substrate, model_name: str, machine_name: str, ip_change: bool = False
) -> None:
    """Cut network from a lxc container.

    Args:
        juju: Juju client
        substrate: The substrate the test is running on
        model_name: The juju model name (only applicable for k8s)
        machine_name: lxc container hostname or k8s pod name
        ip_change: Whether to change the IP address of the unit on the network cut (only applicable for VMs)
    """
    if substrate == Substrate.VM:
        if ip_change:
            lxd_cut_network_from_unit_with_ip_change(machine_name)
        else:
            lxd_cut_network_from_unit_without_ip_change(machine_name)
    else:
        k8s_cut_network_from_unit_without_ip_change(model_name, machine_name)


def restore_network_to_unit(
    substrate: Substrate, model_name: str, machine_name: str, ip_change: bool = False
) -> None:
    """Restore network from a lxc container.

    Args:
        substrate: The substrate the test is running on
        model_name: The juju model name (only applicable for k8s)
        machine_name: lxc container hostname or k8s pod name
        ip_change: Whether the network cut changed the IP address of the unit (only applicable for VMs)
    """
    if substrate == Substrate.VM:
        if ip_change:
            # remove mask from eth0
            restore_network_command = f"lxc config device remove {machine_name} eth0"
            subprocess.check_call(restore_network_command.split())
            return
        lxd_restore_network_to_unit_without_ip_change(machine_name)
    else:
        env = os.environ
        env["KUBECONFIG"] = os.path.expanduser("~/.kube/config")
        subprocess.check_output(
            f"sudo k8s kubectl -n {model_name} delete networkchaos network-loss-primary",
            shell=True,
            env=env,
        )


def deploy_chaos_mesh(namespace: str) -> None:
    """Deploy chaos mesh to the provided namespace.

    Chaos mesh can them be used by the tests to simulate a variety of failures.

    Args:
        namespace: The namespace to deploy chaos mesh to
    """
    env = os.environ
    env["KUBECONFIG"] = os.path.expanduser("~/.kube/config")

    subprocess.check_output(
        " ".join(
            [
                "tests/integration/ha/helpers/deploy_chaos_mesh.sh",
                namespace,
            ]
        ),
        shell=True,
        env=env,
    )


def destroy_chaos_mesh(namespace: str) -> None:
    """Destroy chaos mesh on a provided namespace.

    Cleans up the test K8S from test related dependencies.

    Args:
        namespace: The namespace to deploy chaos mesh to
    """
    env = os.environ
    env["KUBECONFIG"] = os.path.expanduser("~/.kube/config")

    subprocess.check_output(
        f"tests/integration/ha/helpers/destroy_chaos_mesh.sh {namespace}",
        shell=True,
        env=env,
    )


def get_unit_name_from_primary_ip(
    juju: jubilant.Juju, primary_ip: str, substrate: Substrate
) -> str:
    """Get the container name from the primary endpoint.

    Args:
        juju: Juju client
        primary_ip: The primary endpoint IP address to get the corresponding container name for
        substrate: The substrate the test is running on

    Returns:
        The container name corresponding to the primary endpoint.
    """
    for unit_name, unit in juju.status().apps[APP_NAME].units.items():
        try:
            unit_ip = unit.public_address if substrate == Substrate.VM else unit.address
            if unit_ip == primary_ip:
                return unit_name
        except TimeoutError as e:
            logger.warning(f"Failed to get private address for {unit_name}: {e}")
    raise ValueError(f"No unit found with IP address {primary_ip}")


def is_unit_reachable_k8s(namespace: str, source_pod_name: str, to_host: str) -> bool:
    """Test network reachability to a unit in k8s by creating a temporary pod with the same labels as the source pod and trying to ping the destination IP."""
    # ---------------------------------------------------------
    # 1. Setup Client and Bypass SSL (for local/testing clusters)
    # ---------------------------------------------------------
    config.load_kube_config()

    configuration = client.Configuration.get_default_copy()
    configuration.verify_ssl = False
    client.Configuration.set_default(configuration)
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    v1 = client.CoreV1Api()

    # ---------------------------------------------------------
    # 2. Fetch Labels from the Source Pod
    # ---------------------------------------------------------
    try:
        source_pod = v1.read_namespaced_pod(name=source_pod_name, namespace=namespace)
        source_labels = source_pod.metadata.labels or {}
        logger.info(f"Fetched labels from {source_pod_name}: {source_labels}")
    except ApiException as e:
        logger.error(f"Failed to read source pod {source_pod_name}: {e}")
        return False

    # ---------------------------------------------------------
    # 3. Define the Temporary Test Pod
    # ---------------------------------------------------------
    temp_pod_name = f"netshoot-test-{int(time.time())}"

    pod_manifest = client.V1Pod(
        metadata=client.V1ObjectMeta(
            name=temp_pod_name,
            namespace=namespace,
            labels=source_labels,  # <--- Injecting the source pod's labels here
        ),
        spec=client.V1PodSpec(
            restart_policy="Never",
            containers=[
                client.V1Container(
                    name="netshoot",
                    image="nicolaka/netshoot",
                    # Ping five times (-c 5), wait up to 2 seconds for a response (-W 2)
                    command=["ping", "-c", "5", "-W", "2", to_host],
                )
            ],
        ),
    )

    # ---------------------------------------------------------
    # 4. Execute and Wait for Results
    # ---------------------------------------------------------
    try:
        logger.info(f"Creating test pod '{temp_pod_name}' to ping {to_host}...")
        v1.create_namespaced_pod(namespace=namespace, body=pod_manifest)

        # Poll the pod status until it completes
        phase = None
        for attempt in Retrying(stop=stop_after_attempt(30), wait=wait_fixed(2), reraise=True):
            with attempt:
                pod_status = v1.read_namespaced_pod(name=temp_pod_name, namespace=namespace)
                phase = pod_status.status.phase

                if phase not in ["Succeeded", "Failed"]:
                    logger.info(
                        f"Pod '{temp_pod_name}' is in phase '{phase}'. Waiting for completion..."
                    )
                    raise ValueError("Pod not completed yet")

        # Optional: Fetch the actual ping output logs for debugging
        logs = v1.read_namespaced_pod_log(name=temp_pod_name, namespace=namespace)
        logger.info(f"Ping Output:\n{logs.strip()}")

        # If phase is Succeeded, the ping command returned exit code 0
        is_reachable = phase == "Succeeded"

        if is_reachable:
            logger.info(f"Success: {to_host} is reachable from {source_pod_name}.")
        else:
            logger.error(f"Failure: {to_host} is NOT reachable from {source_pod_name}.")

        return is_reachable

    except ApiException as e:
        logger.error(f"Exception during pod creation/execution: {e}")
        return False

    # ---------------------------------------------------------
    # 5. Clean Up (Always runs, even if errors occur above)
    # ---------------------------------------------------------
    finally:
        logger.info(f"Cleaning up pod '{temp_pod_name}'...")
        try:
            v1.delete_namespaced_pod(name=temp_pod_name, namespace=namespace)
        except ApiException as e:
            logger.error(f"Failed to delete temporary pod {temp_pod_name}: {e}")


def is_unit_reachable_lxd(from_host: str, to_host: str, number_of_retries: int = 10) -> bool:
    """Test network reachability between LXD hosts."""
    try:
        for attempt in Retrying(stop=stop_after_attempt(number_of_retries), wait=wait_fixed(10)):
            with attempt:
                ping = subprocess.call(
                    f"lxc exec {from_host} -- ping -c 5 -W 2 {to_host}".split(),
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                if ping == 0:
                    return True
                else:
                    raise ValueError
    except RetryError:
        return False
    return False


def is_unit_reachable(
    juju: jubilant.Juju,
    from_host: str,
    to_host: str,
    substrate: Substrate,
    number_of_retries: int = 10,
) -> bool:
    """Test network reachability to a unit based on the substrate."""
    assert juju.model, "Juju client must be connected to a model before checking unit reachability"
    match substrate:
        case Substrate.K8S:
            return is_unit_reachable_k8s(juju.model, from_host, to_host)
        case Substrate.VM:
            return is_unit_reachable_lxd(from_host, to_host, number_of_retries=number_of_retries)


def hostname_from_unit(juju: jubilant.Juju, unit_name: str) -> str:
    """Get the machine hostname from a specific unit.

    Args:
        juju: An instance of Jubilant's Juju class on which to run Juju commands
        unit_name: The name of the unit to get the machine

    Returns:
        The hostname of the machine.
    """
    task_result = juju.exec(command="hostname", unit=unit_name)

    return task_result.stdout.strip()


def get_sans_from_certificate(certificate_path: str) -> dict[str, set[str]]:
    """Get the SANs for a unit's cert."""
    sans_ip = set()
    sans_dns = set()
    if not (
        san_lines := subprocess.run(
            [
                "openssl",
                "x509",
                "-ext",
                "subjectAltName",
                "-noout",
                "-in",
                certificate_path,
            ],
            capture_output=True,
            text=True,
        ).stdout.splitlines()
    ):
        return {"sans_ip": sans_ip, "sans_dns": sans_dns}

    for line in san_lines:
        for sans in line.split(", "):
            san_type, san_value = sans.split(":")

            if san_type.strip() == "DNS":
                sans_dns.add(san_value)
            if san_type.strip() == "IP Address":
                sans_ip.add(san_value)

    return {"sans_ip": sans_ip, "sans_dns": sans_dns}


def lxd_get_controller_hostname(juju: jubilant.Juju) -> str:
    """Return controller machine hostname."""
    raw_model = juju.cli("show-model", juju.model, include_model=False)
    raw_controller = juju.cli("show-controller", include_model=False)

    model_details = yaml.safe_load(raw_model)
    controller_details = yaml.safe_load(raw_controller)
    controller_name = model_details[juju.model]["controller-name"]

    return [
        machine.get("instance-id")
        for machine in controller_details[controller_name]["controller-machines"].values()
    ][0]


def is_endpoint_in_sentinels(
    juju: jubilant.Juju,
    endpoint: str,
    hostname: str,
    status: str = "",
    tls_enabled: bool = False,
) -> bool:
    """Check if the provided endpoint is present in the sentinels list of any of the provided hostnames."""
    # Sentinel announces bare IPs on VM and FQDNs on K8s (`<pod>.<app>-endpoints.<ns>.svc...`),
    # so accept the endpoint itself or the endpoint as a leading DNS label. A plain substring
    # test is not enough: on VM, "10.70.115.24" is a prefix of a peer at "10.70.115.248", and
    # `SENTINEL SENTINELS` iterates a hash dict, so the peer can be the first "match" and its
    # flags get checked instead of the old primary's.
    endpoint_sentinel = [
        sentinel
        for sentinel in get_sentinels(juju, primary_ip=hostname, tls_enabled=tls_enabled)
        if sentinel["ip"] == endpoint or sentinel["ip"].startswith(f"{endpoint}.")
    ]
    if not endpoint_sentinel:
        logger.error(
            f"Endpoint {endpoint} not found in sentinels list of {hostname}. Sentinels list: {get_sentinels(juju, primary_ip=hostname, tls_enabled=tls_enabled)}"
        )
        return False
    if status and status not in endpoint_sentinel[0]["flags"]:
        logger.error(
            f"Endpoint {endpoint} found in sentinels list of {hostname} but with unexpected status. Expected status: {status}, Sentinels list: {get_sentinels(juju, primary_ip=hostname, tls_enabled=tls_enabled)}"
        )
        return False

    return True


def send_process_control_signal(
    unit_name: str,
    model_full_name: str,
    signal: str,
    db_process: str,
    substrate: Substrate,
) -> None:
    """Send control signal to a database process running on a Juju unit.

    Args:
        unit_name: the Juju unit running the process
        model_full_name: the Juju model for the unit
        signal: the signal to issue, e.g `SIGKILL`
        db_process: the path to the database process binary
        substrate: the substrate the test is running on
    """
    if substrate == Substrate.K8S:
        # For k8s, we exec into the pod and send the signal to the process
        command = f"JUJU_MODEL={model_full_name} juju ssh --container valkey {unit_name} pkill --signal {signal} {db_process}"
    else:
        command = f"JUJU_MODEL={model_full_name} juju ssh {unit_name} -- sudo -i 'pkill --signal {signal} {db_process}'"

    try:
        subprocess.check_output(
            command,
            stderr=subprocess.PIPE,
            shell=True,
            universal_newlines=True,
            timeout=SSH_COMMAND_TIMEOUT,
        )
    except subprocess.TimeoutExpired as e:
        # only the local `juju ssh` was killed -- the remote pkill may have run already
        logger.error(
            "`juju ssh` did not return within %ss while sending %s to %s on unit %s; "
            "the signal may or may not have been delivered",
            SSH_COMMAND_TIMEOUT,
            signal,
            db_process,
            unit_name,
        )
        logger.error("Error details: %s", e)
        raise
    except subprocess.CalledProcessError as e:
        logger.error(
            "failed to send signal %s to process %s on unit %s", signal, db_process, unit_name
        )
        logger.error("Error details: %s", e)
        raise
    logger.info(f"Signal {signal} sent to database process on unit {unit_name}.")
    time.sleep(3)  # give some time for the signal to take effect before the test continues


def lxd_patch_restart_delay(juju: jubilant.Juju, unit_name: str, delay: int | None = None) -> None:
    """Update the restart delay in the snap's systemd service file."""
    delay = delay or VM_RESTART_DELAY_DEFAULT
    juju.exec(
        command=f"sed -i 's/^RestartSec=.*/RestartSec={delay}s/' /etc/systemd/system/{VALKEY_SNAP_SERVICE_NAME}",
        unit=unit_name,
    )

    # reload the daemon for systemd to reflect changes
    juju.exec(command="sudo systemctl daemon-reload", unit=unit_name)


def _pod_exec(
    kube_client: client.api.core_v1_api.CoreV1Api,
    namespace: str,
    pod_name: str,
    container_name: str,
    command: list[str],
    timeout: int = 30,
) -> tuple[int | None, str]:
    """Run a command in a pod container and return (returncode, combined output).

    A returncode of None means the command did not complete within the timeout.
    """
    response = stream.stream(
        kube_client.connect_get_namespaced_pod_exec,
        pod_name,
        namespace,
        container=container_name,
        command=command,
        stdin=False,
        stdout=True,
        stderr=True,
        tty=False,
        _preload_content=False,
    )
    response.run_forever(timeout=timeout)
    return response.returncode, response.read_all()


def pebble_patch_restart_delay(
    juju: jubilant.Juju,
    unit_name: str,
    delay: int | None = None,
    ensure_replan: bool = False,
) -> None:
    """Modify the pebble restart delay of the underlying process.

    Args:
        juju: An instance of Jubilant's Juju class on which to run Juju commands
        unit_name: The name of unit to extend the pebble restart delay for
        delay: The new restart delay to apply
        ensure_replan: Whether to check that the replan command succeeded
    """
    pebble_file_content = (
        EXTEND_PEBBLE_RESTART_DELAY_YAML.format(delay=delay)
        if delay
        else RESTORE_PEBBLE_RESTART_DELAY_YAML
    )
    config.load_kube_config()
    kube_client = client.api.core_v1_api.CoreV1Api()

    pod_name = unit_name.replace("/", "-")
    container_name = "valkey"
    service_name = "valkey"
    layer_path = f"/tmp/pebble_plan_{datetime.now().isoformat()}.yml"

    # A one-shot base64 write avoids the tar-over-stdin websocket dance, whose
    # close() raced tar's read and could leave a truncated layer file behind.
    encoded = base64.b64encode(pebble_file_content.encode()).decode()
    for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3), reraise=True):
        with attempt:
            returncode, output = _pod_exec(
                kube_client,
                juju.model,
                pod_name,
                container_name,
                ["sh", "-c", f"echo {encoded} | base64 -d > {layer_path}"],
            )
            assert returncode == 0, (
                f"Failed to write pebble layer file, unit={unit_name}, "
                f"container={container_name}, returncode={returncode}, output={output!r}"
            )

            returncode, output = _pod_exec(
                kube_client,
                juju.model,
                pod_name,
                container_name,
                ["/charm/bin/pebble", "add", "--combine", service_name, layer_path],
            )
            assert returncode == 0, (
                f"Failed to add to pebble layer, unit={unit_name}, "
                f"container={container_name}, service={service_name}, "
                f"returncode={returncode}, output={output!r}"
            )

    for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3), reraise=True):
        with attempt:
            returncode, output = _pod_exec(
                kube_client,
                juju.model,
                pod_name,
                container_name,
                ["/charm/bin/pebble", "replan"],
                timeout=60,
            )
            if ensure_replan:
                assert returncode == 0, (
                    f"Failed to replan pebble layer, unit={unit_name}, "
                    f"container={container_name}, service={service_name}, "
                    f"returncode={returncode}, output={output!r}"
                )


def patch_restart_delay(
    juju: jubilant.Juju, unit_name: str, delay: int | None, substrate: Substrate
) -> None:
    """Update the restart delay for the database process based on the substrate."""
    match substrate:
        case Substrate.VM:
            lxd_patch_restart_delay(juju, unit_name, delay)
        case Substrate.K8S:
            pebble_patch_restart_delay(juju, unit_name, delay=delay, ensure_replan=True)


def reboot_unit(juju: jubilant.Juju, unit_name: str, substrate: Substrate) -> None:
    """Reboot a unit."""
    if substrate == Substrate.VM:
        juju.exec(command="sudo reboot", unit=unit_name)
    else:
        delete_pod(unit_name.replace("/", "-"), juju.model)


def delete_pod(pod_name: str, namespace="testing") -> None:
    """Delete a pod from the cluster."""
    # Load the kubeconfig file from your local machine (~/.kube/config)
    # Note: If running this script INSIDE a pod, use config.load_incluster_config() instead.
    config.load_kube_config()

    configuration = client.Configuration.get_default_copy()
    configuration.verify_ssl = False
    client.Configuration.set_default(configuration)
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # CoreV1Api contains the methods for core resources like Pods, Services, etc.
    v1 = client.CoreV1Api()

    try:
        # Call the API to delete the pod
        logger.info("Attempting to delete pod %s in namespace '%s'...", pod_name, namespace)
        v1.delete_namespaced_pod(name=pod_name, namespace=namespace)

        logger.info("Success! Pod deleted.")

    except ApiException as e:
        # Handle API errors (e.g., pod not found, unauthorized, etc.)
        if e.status == 404:
            logger.warning("Error: Pod '%s' not found in namespace '%s'.", pod_name, namespace)
        else:
            logger.error("Exception when calling CoreV1Api->delete_namespaced_pod: %s", e)


def instance_ip(model: str, instance: str) -> str:
    """Translate juju instance name to IP.

    Args:
        model: The name of the model
        instance: The name of the instance

    Returns:
        The (str) IP address of the instance
    """
    output = subprocess.check_output(f"juju machines --model {model}".split())

    for line in output.decode("utf8").splitlines():
        if instance in line:
            return line.split()[2]

    return ""


@retry(stop=stop_after_attempt(60), wait=wait_fixed(15), reraise=True)
def wait_network_restore(
    juju: jubilant.Juju,
    substrate: Substrate,
    model_name: str,
    app_name: str,
    hostname: str,
    old_ip: str,
    ip_change: bool = True,
    unit_count: int | None = None,
) -> None:
    """Wait until network is restored.

    Args:
        juju: Juju client
        substrate: The substrate the test is running on (VM or k8s)
        model_name: The name of the model
        app_name: The name of the application
        hostname: The name of the instance
        old_ip: old registered IP address
        ip_change: Whether to check for IP change
        unit_count: The expected number of units for the application (optional)
    """
    if substrate == Substrate.VM and ip_change:
        if instance_ip(model_name, hostname) == old_ip:
            raise Exception("Network not restored, IP address has not changed yet.")
    else:
        # Wait for the network to be restored
        juju.wait(
            lambda status: are_apps_active_and_agents_idle(
                status, app_name, unit_count=unit_count, idle_period=30
            )
        )
