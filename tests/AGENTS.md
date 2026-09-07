# Integration & spread test guidance

Extends the root `AGENTS.md`. (`CLAUDE.md` and `GEMINI.md` in this directory are symlinks to this
file so that Claude Code and Gemini CLI pick it up too.)

- `tox run -e integration -- <test-path> --substrate {k8s,vm}` — a test path IS honored here
  (unlike the unit env); `--substrate` defaults to `k8s` (defined in `tests/conftest.py`).
- Prerequisites: a bootstrapped Juju controller (Canonical K8s for k8s, lxd for vm), a built charm at
  the repo root, and the built requirer-charm
  (`charmcraft pack` inside `tests/integration/clients/requirer-charm/` →
  `requirer-charm_ubuntu@24.04-<arch>.charm`). The requirer-charm ("glide-runner") drives
  continuous writes with `valkey-glide` to validate HA scenarios.
- The integration env's `commands_pre` runs `sudo apt install wget`, downloads a Valkey tarball
  from download.valkey.io, and installs `valkey-cli` into `/usr/local/bin` — all before any
  controller check. The integration dependency group installs entirely from PyPI wheels — no
  protobuf headers or Rust toolchain are needed to build it.
- When `$CI` is unset, tox runs `juju add-model testing` — a second local run fails if the model
  still exists; `juju destroy-model testing` between runs, or invoke pytest directly from
  `.tox/integration/bin/pytest`.
- Waits are long by design: `juju.wait_timeout = 1000` seconds in `tests/integration/conftest.py`
  — a run sitting quiet for 15+ minutes is usually a legitimate wait, not a hang.
- Spread: each `tests/spread/{k8s,vm}/<test>.py/task.yaml` just calls the tox command above on a
  concierge-prepared backend. The `github-ci` backend is `manual: true` (CI-only, one runner per
  job, ~75-min/job timeout); the local backend is `lxd-vm`. CI pins Juju via
  `CONCIERGE_JUJU_CHANNEL: 3.6/stable` and the `k8s` snap `1.32-classic/stable`
  (`concierge-k8s.yaml`) — integration tests moved off MicroK8s, so nothing may shell out to
  `microk8s`.
- Backup fixtures run their object store as a **host service** reached at the host's routable IP
  — never loopback (from a unit that resolves to the unit itself) and never a container, since
  the workflow purges Docker so Canonical K8s can install. MicroCeph (snap) serves S3, Azurite
  (npm on the `node` snap) serves Azure; both are declared under `host.snaps` in
  `concierge-{k8s,vm}.yaml` and self-installed by the fixture as a local fallback, so one code
  path covers both substrates.
