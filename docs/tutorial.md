(tutorial)=

# Tutorial

This hands-on tutorial aims to help you learn how to deploy Charmed Valkey and 
become familiar with its available operations.

## Prerequisites

While this tutorial intends to guide you as you deploy Charmed Valkey for the
first time, it will be most beneficial if:

- You have some experience using a Linux-based CLI
- You are familiar with the [Juju orchestration engine](https://documentation.ubuntu.com/juju/latest/)

### Minimum system requirements

- Any Linux operating system that supports snaps
- At least 4GB of RAM
- At least 4 CPUs
- At least 20GB of available storage
- Virtualisation support
- amd64 or arm64 architecture

## Set up the environment

Charmed Valkey can be deployed on both Kubernetes and VM clouds. For this tutorial,
we will deploy Charmed Valkey on Kubernetes using MicroK8s.

First, we will set up a cloud environment using [Multipass](https://multipass.run/)
with [MicroK8s](https://microk8s.io/docs) and [Juju](https://documentation.ubuntu.com/juju/3.6/). 
This is the quickest and easiest way to get your machine ready for using Charmed Valkey
on Kubernetes. If you are interested in VM deployments, [LXD](https://documentation.ubuntu.com/lxd/latest/)
can be used instead of MicroK8s. 

### Multipass

[Multipass](https://multipass.run/) is a quick and easy way to launch virtual
machines running Ubuntu. It uses the [cloud-init](https://cloud-init.io/) standard
to install and configure all the necessary parts automatically.

Install Multipass from the [snap store](https://snapcraft.io/multipass):

```shell
sudo snap install multipass
```

Spin up a new VM using [`multipass launch`](https://multipass.run/docs/launch-command)
with the [charm-dev](https://github.com/canonical/multipass-blueprints/blob/main/v1/charm-dev.yaml)
cloud-init configuration:

```shell
multipass launch --cpus 4 --memory 4G --disk 20G --name dev-vm charm-dev
```

As soon as a new VM has started, access it:

```shell
multipass shell dev-vm
```

```{tip}
If at any point you'd like to leave a Multipass VM, enter `Ctrl+D` or type `exit`.
```

All necessary components have been pre-installed inside the VM already, like LXD and Juju.

### Juju

Let's bootstrap Juju to use the local MicroK8s controller. We will call it 
"k8s-controller", but you can give it any name you'd like:

```shell
juju bootstrap microk8s k8s-controller
```

If you decided to deploy an LXD environment instead of MicroK8s, bootstrap an LXD
controller instead.

<details> <summary> Bootstrap example for LXD</summary>

```shell
juju bootstrap localhost lxd-controller
```

</details>

For this tutorial, we will continue with the MicroK8s environment, but all commands
can equally be run on the LXD environment as well.

A controller can work with different [models](https://juju.is/docs/juju/model). 
Set up a specific model for Charmed Valkey named `tutorial`:

```shell
juju add-model tutorial
```

You can now view the model you created above by running the command `juju status`. You should see something similar to the following example output:

```text
Model     Controller      Cloud/Region        Version  SLA          Timestamp
tutorial  k8s-controller  microk8s/localhost  3.6.14   unsupported  12:48:05+01:00

Model "admin/tutorial" is empty.
```

## Deploy Valkey

To deploy Charmed Valkey, run:

```shell
juju deploy valkey --channel 9/edge --trust
```

Juju will now fetch Charmed Valkey from [Charmhub](https://charmhub.io/valkey) and
deploy it to the local MicroK8s. This process can take a few minutes depending
on how provisioned (RAM, CPU, etc.) your machine is. 

You can track the progress by running:

```shell
watch juju status --color
```

This command is useful for checking the real-time information about the state
of a charm and the machines hosting it. Check the [`juju status` documentation](https://juju.is/docs/juju/juju-status)
for more information about its usage.

When the application is ready, `juju status` will show something similar to the
sample output below:

```text
Model     Controller      Cloud/Region        Version  SLA          Timestamp
tutorial  k8s-controller  microk8s/localhost  3.6.14   unsupported  12:53:52+01:00

App     Version  Status  Scale  Charm   Channel  Rev  Address         Exposed  Message
valkey           active      1  valkey  9/edge    11  10.152.183.123  no       

Unit       Workload  Agent  Address      Ports  Message
valkey/0*  active    idle   10.1.44.126         
```

You can also watch Juju logs with the [`juju debug-log`](https://juju.is/docs/juju/juju-debug-log) command.

## Access Valkey

In this section, you will learn how to get the credentials of your deployment and
connect to the Valkey database.

```{caution}
This part of the tutorial uses the charmed-operator user to access Valkey.

**Do not use the charmed-operator user directly in a production environment.**
```

### Retrieve credentials

Connecting to the database requires that you know two pieces of information: 

1. The internal Valkey database user credentials (username and password)
2. The host machine's IP address. 

Check the IP addresses associated with each application unit with the `juju status` command: 

```text
Unit       Workload  Agent  Address      Ports  Message
valkey/0*  active    idle   10.1.44.126         
```

The user we will connect to in this tutorial will be the internal `charmed-operator`
user of Charmed Valkey. To retrieve its associated password, run the following command:

```shell
juju show-secret valkey-peers.valkey.app.internal_users_secret --reveal
```

Copy the content displayed for `charmed-operator-password`.

### Access the database

The easiest way to interact with Valkey is via [its command line interface `valkey-cli`](https://valkey.io/topics/cli/).
which can be installed with the `valkey-tools` package in Ubuntu:

```shell
sudo apt update && sudo apt install valkey-tools
```

Run the command below to connect to your Charmed Valkey database, using the unit's IP address:

```shell
valkey-cli -h 10.1.44.126 -p 6379
```

In Kubernetes environments, it is also possible to connect to the database using the DNS name
of the unit. For that, connect into the unit:

```shell
juju ssh --container valkey valkey/0
```

Use the DNS endpoint to access Valkey from within the unit:

```shell
valkey-cli -h valkey-0.valkey-endpoints -p 6379
```

Run the following command to log in, using the previously retrieved credentials:

```shell
10.1.44.126:6379> AUTH charmed-operator <your-password-here>
```

Now perform a basic health check with this command:

```shell
10.1.44.126:6379> ping
```

You should receive this response from the Valkey server:

```text
PONG
```

Now it is possible to perform Valkey commands on the database. To set a key `mykey`
to the value `HelloWorld`:

```shell
10.1.44.126:6379> set mykey "HelloWorld"
```

In order to retrieve the key you just set, run the following command:

```shell
10.1.44.126:6379> get mykey
```

As response, you should get the value you just set:

```
"HelloWorld"
```

Please refer to the [command reference of `valkey-cli`](https://valkey.io/commands/)
for further information about available commands.

## Scale your deployment

The Charmed Valkey operator uses Valkey's [asynchronous replication](https://valkey.io/topics/replication/)
and [Sentinel](https://valkey.io/topics/sentinel/) to achieve High Availability. 
It provides features such as automatic primary-replica management, fault tolerance,
and automatic failover.

In order to enable these features, add additional nodes to your Valkey deployment.

```{caution}
This tutorial hosts all nodes on the same machine. 

**This should not be done in a production environment.** 
```

### Add units

You can add replica units to your deployed Valkey database with the following command:

```shell
juju add-unit valkey -n 2
```

Where `-n 2` specifies the number of units to add. In this case, we are adding
two units to Valkey.

You can now watch the new units join the deployment with `watch juju status`.
It usually takes a few minutes for the new units to be added to the cluster formation.
You’ll know that all units are ready when `juju status` reports:

```text
Model     Controller      Cloud/Region        Version  SLA          Timestamp
tutorial  k8s-controller  microk8s/localhost  3.6.14   unsupported  13:55:46+01:00

App     Version  Status  Scale  Charm   Channel  Rev  Address         Exposed  Message
valkey           active      3  valkey  9/edge    11  10.152.183.123  no       

Unit       Workload  Agent  Address      Ports  Message
valkey/0*  active    idle   10.1.44.126         
valkey/1   active    idle   10.1.44.117         
valkey/2   active    idle   10.1.44.110         
```

### Remove units

Removing a unit from the application scales down the replicas. If you currently have
three units, one is the primary and two are replicas. Removing a unit will reduce the
number of replicas to one.

Before scaling down, list all the units with `juju status`. You will see 
three units: 

* `valkey/0`
* `valkey/1`
* `valkey/2` 

To scale the application down to two units, enter:

```shell
juju remove-unit valkey --num-units 1
```

You’ll know that the unit was successfully removed when `juju status` reports:

```text
Model     Controller      Cloud/Region        Version  SLA          Timestamp
tutorial  k8s-controller  microk8s/localhost  3.6.14   unsupported  14:39:39+01:00

App     Version  Status  Scale  Charm   Channel  Rev  Address         Exposed  Message
valkey           active      2  valkey  9/edge    11  10.152.183.123  no       

Unit       Workload  Agent  Address      Ports  Message
valkey/0*  active    idle   10.1.44.126         
valkey/1   active    idle   10.1.44.117         
```

## Enable encryption with TLS

[Transport Layer Security (TLS)](https://en.wikipedia.org/wiki/Transport_Layer_Security) 
is a protocol used to encrypt data exchanged between two applications. Essentially,
it secures data transmitted over a network.

Typically, enabling TLS internally within a highly available database or between
a highly available database and client/server applications requires a high level
of expertise. This has all been encoded into Charmed Valkey so that configuring
TLS requires minimal effort on your end.

TLS is enabled by integrating Charmed Valkey with the [Self-signed certificates charm](https://charmhub.io/self-signed-certificates). 
This charm centralises TLS certificate management consistently and handles operations
like providing, requesting, and renewing TLS certificates.

```{caution}
**[Self-signed certificates](https://en.wikipedia.org/wiki/Self-signed_certificate) are not recommended for a production environment.**

Check [this guide](https://discourse.charmhub.io/t/security-with-x-509-certificates/11664) for an overview of the TLS certificates charms available. 
```

Before enabling TLS on Charmed Valkey, we must deploy the `self-signed-certificates` operator:

```shell
juju deploy self-signed-certificates --config ca-common-name="Tutorial CA"
```

Wait until the `self-signed-certificates` is up and active, use `juju status` to 
monitor the progress:

```text
Model     Controller      Cloud/Region        Version  SLA          Timestamp
tutorial  k8s-controller  microk8s/localhost  3.6.14   unsupported  14:45:41+01:00

App                       Version  Status  Scale  Charm                     Channel   Rev  Address         Exposed  Message
self-signed-certificates           active      1  self-signed-certificates  1/stable  586  10.152.183.111  no       
valkey                             active      2  valkey                    9/edge     11  10.152.183.123  no       

Unit                         Workload  Agent  Address      Ports  Message
self-signed-certificates/0*  active    idle   10.1.44.89          
valkey/0*                    active    idle   10.1.44.126         
valkey/1                     active    idle   10.1.44.117         
```

To enable TLS on Charmed Valkey, integrate the two applications:

```shell
juju integrate valkey:client-certificates self-signed-certificates:certificates
```

The Charmed Valkey operator will now enable client TLS in Valkey. You can watch 
the progress with `juju status`:

```text
Model     Controller      Cloud/Region        Version  SLA          Timestamp
tutorial  k8s-controller  microk8s/localhost  3.6.14   unsupported  14:53:02+01:00

App                       Version  Status       Scale  Charm                     Channel   Rev  Address         Exposed  Message
self-signed-certificates           active           1  self-signed-certificates  1/stable  586  10.152.183.111  no       
valkey                             maintenance      2  valkey                    9/edge     11  10.152.183.123  no       Enabling client TLS...

Unit                         Workload     Agent      Address      Ports  Message
self-signed-certificates/0*  active       idle       10.1.44.89          
valkey/0*                    maintenance  executing  10.1.44.126         Enabling client TLS...
valkey/1                     maintenance  executing  10.1.44.117         Enabling client TLS...
```

Please note that Charmed Valkey switches the port for client connections when enabling
client TLS. Valkey now listens on port `6380` (previously it was `6379`).

Use `openssl` to connect to Valkey and check the TLS certificate in use. Note 
that your unit's IP address will likely be different to the one shown below:

```shell
openssl s_client -connect 10.1.44.126:6380  2>/dev/null | openssl x509 -noout -dates
```

You should see the validity dates of the certificate in use as output:

```text
notBefore=Mar 16 13:52:57 2026 GMT
notAfter=Jun 14 13:52:57 2026 GMT
```

Congratulations! Valkey is now using a TLS certificate generated by the external
application `self-signed-certificates`.

To remove the external TLS, remove the integration:

```shell
juju remove-relation valkey:client-certificates self-signed-certificates:certificates
```

The Charmed Valkey application is not using TLS anymore.

## Clean up your environment

In this tutorial we've successfully deployed Valkey on MicroK8s, added and removed
cluster members, and enabled a layer of security with TLS.

You may now keep your Charmed Valkey deployment running and write to the database 
or remove it entirely using the steps in this page. 

If you'd like to keep your environment for later, simply leave your VM with `Ctrl+D`
and stop it with:

```shell
multipass stop dev-vm
```

If you're done with testing and would like to free up resources on your machine, 
you can remove the VM entirely.

```{warning}
When you remove VM as shown below, you will lose all the data in Valkey and any other applications inside Multipass VM! 

For more information, see the docs for [`multipass delete`](https://multipass.run/docs/delete-command).
```

**Delete your VM and its data** by running:

```shell
multipass delete --purge dev-vm
```
