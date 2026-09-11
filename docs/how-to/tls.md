# How to enable TLS

Valkey provides a secure transport layer for communication between clients and Valkey,
and the charm provides a simple way of enabling TLS encryption.

## Peer-to-peer connections

For internal communication between Valkey peers, Charmed Valkey always enables TLS
by default. The TLS certificates for this purpose are managed by the charm itself
and do not require a separate TLS provider. This applies to connections between
Valkey primary and replicas as well as between Valkey and Sentinel and between 
Sentinel instances.

To add another layer of security for communication between clients and Valkey,
follow the instructions in the next sections. The self-managed, internal peer TLS
will be replaced with an external TLS provider and encryption will be enabled 
for both client and peer TLS.

## Deploy a TLS provider

This guide will use the [Self-signed Certificates](https://charmhub.io/self-signed-certificates)
charm as an example for all cases.

```{caution}
**[Self-signed certificates](https://en.wikipedia.org/wiki/Self-signed_certificate) are not recommended for a production environment.**

Check the [Choosing a TLS provider](https://canonical-certificate-management.readthedocs-hosted.com/operator/understanding-tls/#choosing-a-tls-provider) page for an overview of all the TLS certificates charms available. 
```

Deploy the `self-signed-certificates` charm:

```shell
juju deploy self-signed-certificates
```

```{warning}
Charmed Valkey uses `v4` of the
[{spellexception}`tls-certificates` library](https://charmhub.io/tls-certificates-interface/libraries/tls_certificates).
Earlier versions of the library are not supported.
```

Wait until `self-signed-certificates` is `active` by monitoring it with `juju status`.

```text
App                       Version  Status  Scale  Charm                     Channel   Rev  Address         Exposed  Message
self-signed-certificates           active      1  self-signed-certificates  1/stable  586  10.152.183.111  no       
valkey                             active      2  valkey                    9/edge     11  10.152.183.123  no       
```

## Enable client-to-server encryption

To enable client-to-server encryption, you need to generate the required certificates
and keys for the clients.

### Integrate on `client-certificates` endpoint

Integrate the certificates charm with the Charmed Valkey application on the `client-certificates`
endpoint. This will generate the required certificates and keys in the TLS provider and 
send them to Charmed Valkey.

```shell
juju integrate valkey:client-certificates self-signed-certificates:certificates
```

To verify the server's certificate, the `valkey-cli` client needs to know the
certificate authority (CA) that signed the server's certificate. Therefor get the CA
certificate from the `self-signed-certificates` charm using the following command:

```shell
juju run self-signed-certificates/0 get-ca-certificate
```

Save the CA certificate from the output to a file called `ca_cert.pem` on your local machine.

```text
Running operation 1 with 1 task
  - task 2 on unit-self-signed-certificates-0

Waiting for task 2...
ca-certificate: |-
  -----BEGIN CERTIFICATE-----
  ...
  -----END CERTIFICATE-----
```

Now that you have the CA certificate, you can use it to verify the server certificate. 

Remember that the server is configured to also require client TLS. You need to provide
the client certificate and key to authenticate the client. 

For this guide, download the server's certificate and key from a unit of Charmed
Valkey to the local machine.

Download the certificate:

```shell
juju scp --container valkey valkey/1:/var/lib/valkey/tls/client.pem client.pem
```

Download the private key:

```shell
juju scp --container valkey valkey/1:/var/lib/valkey/tls/client.key client.key 
```

Now you have all required files and can provide them as options to the `valkey-cli` command:

```shell
valkey-cli -h 10.1.44.126 -p 6380 --tls --cert client.pem --key client.key --cacert ca_cert.pem
```

You should see an authentication error as result since the connection was established,
but no credentials provided:

```text
(error) NOAUTH Authentication required.
```

Authenticate with username and password to log in:

```text
10.1.44.126:6380> AUTH charmed-operator <PASSWORD>
```

Now you can interact with the database:

```text
10.1.44.126:6380> ping
```

You should receive a response like this from Valkey:

```text
PONG
```

You can now successfully connect to the server using the `tls` directive and provide
the CA certificate, client certificate, and key to verify the server certificate. 

## Certificate expiration and rotation

Certificate rotation is fully automated in Charmed Valkey. No manual effort 
should be needed. If a certificate expires in the next 24 hours, it will display a status:

```text
Model     Controller      Cloud/Region        Version  SLA          Timestamp
tutorial  k8s-controller  microk8s/localhost  3.6.14   unsupported  19:01:55+01:00

App                       Version  Status  Scale  Charm                     Channel   Rev  Address         Exposed  Message
self-signed-certificates           active      1  self-signed-certificates  1/stable  586  10.152.183.111  no       
valkey                             active      3  valkey                    9/edge     11  10.152.183.123  no       TLS certificates expiring soon. Please ensure new certificates are provided

Unit                         Workload  Agent  Address      Ports  Message
self-signed-certificates/0*  active    idle   10.1.44.89          
valkey/0*                    active    idle   10.1.44.126         TLS certificates expiring soon. Please ensure new certificates are provided
valkey/1                     active    idle   10.1.44.117         TLS certificates expiring soon. Please ensure new certificates are provided
valkey/2                     active    idle   10.1.44.127         TLS certificates expiring soon. Please ensure new certificates are provided
```

In addition, Charmed Valkey will write a message to the log: `TLS client/peer certificates expiring soon. Please ensure new certificates are provided`. 
This log message can be used for [creating an alert with COS](https://charmhub.io/topics/canonical-observability-stack/how-to/add-alert-rules).

As soon as new certificates are issued by the TLS provider, Valkey will replace 
the expiring certificate with the renewed one on each unit, reloading the TLS files
into Valkey to ensure continued communication.

## Manage private keys

You can manage private keys used by the charm to generate the certificate signing
requests (CSR) by storing the private key in a [Juju secret](https://canonical-juju.readthedocs-hosted.com/en/latest/user/reference/secret/)
and then referencing the secret in the [charm configuration](https://documentation.ubuntu.com/juju/latest/howto/manage-applications/index.html#configure-an-application).

### Store the private key in a Juju secret

To store the private key in a Juju secret, run the following command:

```shell
juju add-secret tls-private-key private-key=$(base64 -w0 private-key.pem)
```

You can use the secret ID from the output to reference the secret in the charm configuration.

```shell
secret:d6s4hbnmp25c765uceo0
```

Now that the secret is stored, you can grant the secret to the application using the following command:

```shell
juju grant-secret tls-private-key valkey
```

### Reference the secret in the charm configuration

To set the private key for TLS encryption, run:

```shell
juju config valkey tls-client-private-key=secret:d6s4hbnmp25c765uceo0
```

Once the configuration is set, the charm will use the private key stored in the secret
to generate new certificate signing requests (CSR) to acquire new certificates from the TLS provider.

## Disable TLS

In general, to disable encryption with TLS, remove the relation between Valkey and 
the TLS provider on the client-certificates endpoint:

```shell
juju remove-relation valkey:client-certificates self-signed-certificates
```

After some time, you'll see that the relation between `self-signed-certificates` and Valkey
has been removed:

```text
Model     Controller      Cloud/Region        Version  SLA          Timestamp
tutorial  k8s-controller  microk8s/localhost  3.6.14   unsupported  19:08:08+01:00

App                       Version  Status  Scale  Charm                     Channel   Rev  Address         Exposed  Message
self-signed-certificates           active      1  self-signed-certificates  1/stable  586  10.152.183.111  no       
valkey                             active      3  valkey                    9/edge     11  10.152.183.123  no       

Unit                         Workload  Agent  Address      Ports  Message
self-signed-certificates/0*  active    idle   10.1.44.89          
valkey/0*                    active    idle   10.1.44.126         
valkey/1                     active    idle   10.1.44.117         
valkey/2                     active    idle   10.1.44.127         

Integration provider  Requirer             Interface     Type  Message
valkey:status-peers   valkey:status-peers  status_peers  peer  
valkey:valkey-peers   valkey:valkey-peers  valkey_peers  peer  
```

You have successfully disabled encryption with TLS for Valkey. You can verify that
the database is running without encryption by checking the `valkey-cli` command
without the `tls` directive:

```shell
valkey-cli -h 10.1.44.126 -p 6379
```

You should see an authentication error as the result since the network connection was established,
but no credentials provided:

```text
(error) NOAUTH Authentication required.
```

Notice that the database is running without encryption for client connections only.
For internal peer-to-peer communication, Charmed Valkey always uses TLS encryption.