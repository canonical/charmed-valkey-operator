(how-to-backup)=

# How to back up and restore

This guide walks you through protecting your Valkey data: taking point-in-time
backups of your dataset to object storage, listing the backups you have taken,
and restoring the whole cluster from any of them. S3-compatible object storage,
Azure Blob storage, and Google Cloud Storage are supported through their
respective integrators.

The integrators store credentials. Charmed Valkey reads the credentials over the
relation and never stores them in plain text.

```{caution}
Only one storage integrator can be related at a time. Relating multiple
integrators places Charmed Valkey in a blocked state until you remove the extra
relations.
```

## S3-compatible object storage

To use S3-compatible storage for your backups, deploy
[`s3-integrator`](https://charmhub.io/s3-integrator):

```shell
juju deploy s3-integrator --channel 2/stable
```

Store the access key and secret key in a
[Juju secret](https://canonical-juju.readthedocs-hosted.com/en/latest/user/reference/secret/)
and grant access to the integrator:

```shell
juju add-secret s3-creds access-key=<ACCESS_KEY> secret-key=<SECRET_KEY>
juju grant-secret s3-creds s3-integrator
```

Point the integrator at the secret and configure the bucket:

```shell
juju config s3-integrator \
  credentials=secret:<SECRET_ID> \
  bucket=<BUCKET> \
  endpoint=<ENDPOINT_URL> \
  region=<REGION> \
  path=<PATH_PREFIX>
```

If your endpoint uses a private or self-signed certificate, base64-encode its CA
chain and pass it as `tls-ca-chain` so the integrator can verify the endpoint over
HTTPS:

```shell
juju config s3-integrator tls-ca-chain="$(base64 -w0 ca-chain.pem)"
```

Finally, integrate Charmed Valkey with the S3 integrator:

```shell
juju integrate valkey:s3-credentials s3-integrator
```

## Azure Blob storage

To use Azure Blob storage for your backups, deploy
[`azure-storage-integrator`](https://charmhub.io/azure-storage-integrator):

```shell
juju deploy azure-storage-integrator --channel 1/stable
```

Store the storage-account key in a
[Juju secret](https://canonical-juju.readthedocs-hosted.com/en/latest/user/reference/secret/)
and grant access to the integrator:

```shell
juju add-secret azure-creds secret-key=<STORAGE_ACCOUNT_KEY>
juju grant-secret azure-creds azure-storage-integrator
```

```{note}
See [Azure limitations](#azure-limitations) below before choosing an endpoint or
connection protocol for Azure.
```

Point the integrator at the secret and configure the container:

```shell
juju config azure-storage-integrator \
  credentials=secret:<SECRET_ID> \
  container=<CONTAINER> \
  storage-account=<STORAGE_ACCOUNT> \
  connection-protocol=https \
  path=<PATH_PREFIX>
```

Finally, integrate Charmed Valkey with the Azure storage integrator:

```shell
juju integrate valkey:azure-credentials azure-storage-integrator
```

(azure-limitations)=

### Azure limitations

The Azure Blob backend supports public Azure Blob storage over HTTPS and
plain-HTTP emulators such as [Azurite](https://github.com/Azure/Azurite). However,
it does not provide full feature parity with the S3 backend:

- **HTTPS endpoints that use a private or custom CA are not supported.** The
  `azure_storage` relation does not provide a CA-chain field, so Charmed Valkey
  can verify only certificates issued by a public CA in the system trust store.
  It cannot establish a trusted HTTPS connection to an Azure-compatible endpoint,
  such as an Azure Stack deployment, that uses a private or self-signed
  certificate. If you require a custom CA, use the S3 backend and configure
  `tls-ca-chain` instead.
- **The `abfs` and `abfss` connection protocols are not supported.** These protocols
  identify ADLS Gen2 hierarchical-namespace endpoints, which use a different API
  from Azure Blob storage. Use a supported Blob protocol instead: `https` or
  `wasbs` for HTTPS, or `http` or `wasb` for HTTP.

## Google Cloud Storage

To use Google Cloud Storage for your backups, deploy
[`gcs-integrator`](https://charmhub.io/gcs-integrator):

```shell
juju deploy gcs-integrator --channel 1/stable
```

Create a service account in your Google Cloud project and download its JSON key.
Grant the service account permission to list, read, and write objects in the
bucket. If the bucket does not exist yet, Charmed Valkey creates it, which also
requires permission to create buckets in the project.

Store the JSON key in a
[Juju secret](https://canonical-juju.readthedocs-hosted.com/en/latest/user/reference/secret/)
under the `secret-key` content key and grant access to the integrator:

```shell
juju add-secret gcs-creds secret-key#file=service-account.json
juju grant-secret gcs-creds gcs-integrator
```

The `#file=` suffix tells Juju to read the value from the local file rather
than from the command-line argument, keeping the private key out of your shell
history.

```{note}
Charmed Valkey accepts service-account keys as plain JSON or base64-encoded JSON, so
no decoding is required.
```

Point the integrator at the secret and configure the bucket:

```shell
juju config gcs-integrator \
  credentials=secret:<SECRET_ID> \
  bucket=<BUCKET> \
  path=<PATH_PREFIX>
```

Finally, integrate Charmed Valkey with the GCS integrator:

```shell
juju integrate valkey:gcs-credentials gcs-integrator
```

## Create a backup

After relating an integrator, wait for it to reach `active` status. Run the
`create-backup` action on **any** unit — leader or follower — to stream that
unit's dataset directly to object storage:

```shell
juju run valkey/leader create-backup
```

On success, the action returns the identifier of the new backup:

```text
backup-id: 2026-07-20T12:30:00Z
```

## List backups

List all backups currently available in the configured bucket or container:

```shell
juju run valkey/leader list-backups
```

Review the table, which lists the newest backup first.
To retrieve machine-readable output, pass `output=json`:

```shell
juju run valkey/leader list-backups output=json
```

The `list-backups` action is read-only and safe to run while another backup is
still uploading.

## Restore a backup

Restoring replaces the dataset on **all** units with the contents of the chosen
backup, so it must run on the **leader** unit.

Copy the `backup-id` exactly as shown by `list-backups`, then run the `restore`
action.

```{caution}
The `restore` action overwrites the dataset on every unit. Before
proceeding, create a fresh backup of the current data.
```

```shell
juju run valkey/leader restore backup-id=2026-07-20T12:30:00Z
```

The action confirms that the restore was initiated:

```text
restore: initiated for 2026-07-20T12:30:00Z
```

Charmed Valkey coordinates the restore across the cluster. Check its progress:

```shell
juju status
```

Verify that every unit returns to `active` status, indicating that each unit has
fully loaded the restored dataset.

## Troubleshooting

If Charmed Valkey does not reach `active` status, check the output of `juju status`
for one of the following messages:

- **`Missing or invalid backup storage credentials`**: The related integrator
  does not provide valid storage credentials or configuration. Check the
  integrator configuration and ensure that `path` is set. Integrators use an
  empty path by default, but Charmed Valkey requires a path to prevent
  `list-backups` from listing an entire bucket or container. If the problem
  persists, inspect `juju debug-log` for the leader unit.
- **`More than one backup storage integrator related; relate exactly one`**: Two
  or more of `s3-integrator`, `azure-storage-integrator`, and `gcs-integrator`
  are related. Remove the extra integrations. Charmed Valkey automatically
  detects and uses the remaining integrator.

If the problems persists, check `juju debug-log` on the leader unit for more details.
