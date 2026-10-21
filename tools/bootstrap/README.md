# Reserved-port bootstrap preparation

Run **`prepare_nccl_bootstrap.py`** to prepare persistent schema-4 bootstrap
listeners. Keep all three Python files together on the launcher:

| File | Role |
| --- | --- |
| `prepare_nccl_bootstrap.py` | Recommended reserved-port-only command |
| `create_static_bootstrap.py` | Shared allocation, controller, and manifest logic; compatibility CLI |
| `check_bootstrap_ports.py` | Shared validation and node-local listener/FD-handoff worker |

Only the first command needs to be launched. It sends the helper implementation
over SSH and starts one helper per node automatically; the Python files do not
need to be installed on every worker.

## Requirements

- Linux and Python 3.10+ on the launcher and all selected nodes; no third-party
  Python packages are required.
- Noninteractive SSH as `azhpcuser` from the launcher to every selected node,
  including itself. Workers need iproute2's `ip` and GNU `timeout`.
- An explicitly approved, freshly observed allocation as described below.
- Four administratively reserved TCP ports on the selected IPv4 address of
  every node. Defaults are `27000 27001 27002 27003`, in local-GPU order.

The existing SSH implementation uses `StrictHostKeyChecking=no`, which weakens
host authentication. Verify node identities and host keys before authorizing
preparation; these tools do not authenticate allocation evidence or owner labels.

Using the manifest with NCCL additionally requires matching patched NCCL and
this plugin's v4 extension across all ranks. The companion NCCL patches are
**not included** in this repository. Preparation itself does not use GPUs.

## Usage

From the repository root, after confirming the allocation and authorizing
listener preparation:

```bash
python3 tools/bootstrap/prepare_nccl_bootstrap.py \
    --allocation approved-allocation.json \
    --output static-bootstrap.json \
    --hold-seconds 1800
```

This starts preparation immediately on **every node** in the allocation, in its
recorded order. Each node contributes four contiguous endpoints. There is no
node-count argument, dry-run mode, alternate-port search, or fallback.

Optional arguments:

- `--ports P0 P1 P2 P3`: four distinct ports in `1..65535`, preserving the supplied
  order and using the same block on each node.
- `--startup-concurrency N`: positive integer, default `128`; effective limit is
  `min(N, node_count)`. This limits helpers awaiting readiness, not the eventual
  number of live helpers or SSH sessions. Ready helpers remain running.

All helpers must be ready before the controller atomically publishes a new
manifest. It never overwrites an existing output, including a symlink. Each
listener is exclusively bound and retained, not temporarily probed then rebound.
An occupied port fails with its hostname, IP, port, operation, and `EADDRINUSE`;
no other process is stopped. Partial preparation cleans its own helpers and
sockets without publishing a manifest. Other failures retain their actual errors.

Stdout is JSON Lines. A `prepared` record means the manifest and helpers are
ready, **not that the command has finished**. Keep the controller and its stdin
attached. Distribute identical manifest bytes to the nodes separately and point
every application rank's `NCCL_STATIC_BOOTSTRAP_FILE` at its node-local copy.
The helpers retain their listeners across fresh full/strided-subset communicators
and transfer connected FDs; they do not relay collective traffic.

After all communicators and workloads are cleaned up, send `STOP\n` on the
controller's stdin. Normal shutdown emits `stopped` and exits `0`.
`--hold-seconds` is required and must be `1..3600`: it is a finite lifetime from
each helper's startup, not a startup delay. Allow time for preparation,
distribution, application execution, and cleanup. EOF, expiry, or failure is an
error, normally exit `2`. Shutdown closes helper-owned resources, not
rank-owned connections, and does not terminate the NCCL application. A manifest
published before a later failure remains as evidence, not proof of live helpers.

These tools do not reserve nodes, set Linux reserved-port policy, install
software, distribute manifests, or launch NCCL. Four ports cover bootstrap
listeners only, not all NCCL/MRC sockets. Independent jobs cannot own the same
port block on the same node IP simultaneously.

## Allocation input

Obtain exact node identities/topology and recorded health through `mrc-mcp`
inventory tools, and reservation identities through its reservation tools.
Preserve source warnings and actual observation timestamps. Do not derive IPs
from hostnames, use an old hostfile, or relabel stale observations as fresh.
Node reservation and release remain separate confirmation-gated operations.

The following is a **template**, not a usable allocation:

```json
{
  "schema_version": 1,
  "source": "mrc-mcp",
  "inventory_observed_at": "<inventory response timestamp with timezone>",
  "reservations_observed_at": "<reservation response timestamp with timezone>",
  "approved_ips": ["<node IPv4 address>"],
  "socket_ifname": "<interface owning that address on every node>",
  "placement": {"kind": "same_row", "row_key": "<observed row key>"},
  "warnings": [],
  "nodes": [
    {
      "ip": "<node IPv4 address>",
      "hostname": "<observed hostname>",
      "rack": "<observed rack>",
      "row_key": "<observed row key>",
      "state": "reserved",
      "owner": "<recorded reservation owner>",
      "until": "<recorded reservation expiry with timezone>",
      "note": "<recorded reservation note, or null>",
      "health": "pass",
      "last_check_at": "<recorded health timestamp with timezone>",
      "operations_eligible": true,
      "reachability": "reachable"
    }
  ]
}
```

Include one complete node record per approved IP, without duplicate IPs or
hostnames. `approved_ips` must identify exactly that pool; **`nodes` determines
the endpoint order**. Replace `warnings` with the actual source warnings.
Placement is explicit: `same_row` with `row_key`, `same_rack` with `rack`, or
`multi_row` with at least two distinct `row_keys` for an approved cross-row pool.
Every node must have complete topology and satisfy the placement constraint.

Inventory and reservation snapshots must be no older than 300 seconds at
validation; recorded health must be PASS no older than 5400 seconds. Nodes must
be reserved, reachable, and operations-eligible. Reservations must cover the
conservative preparation/cleanup budget:
`45 * node_count + 60 + hold_seconds` seconds. Eligibility is rechecked from
the recorded evidence during startup and before publication, but the scripts
do not refresh inventory themselves. Reconcile live allocation state before
preparation and again before launching NCCL.

## Compatibility entrypoints

`create_static_bootstrap.py` retains the older port-selection, schema-2 prepared,
and schema-3/schema-4 persistent modes. Without `--probe` or `--hold-listeners`
it validates and prints a first-N plan without SSH or manifest publication.
Use its `--help` for legacy arguments; the reserved-only command does not accept
those mode flags.

`check_bootstrap_ports.py MANIFEST` validates a saved manifest without contacting
nodes. Its standalone `--probe` supports schema 1 only: schemas 2-4 already have
live listeners. Do not launch its internal worker modes separately.

The reserved-only interface passed local CLI/loopback lifecycle and failure
tests and a four-node script-only smoke test with default concurrency (effective
four), explicit concurrency two, and an occupied-port failure. That smoke test
verified 16 live listeners, manifest publication/error handling, ownership
preservation, and cleanup; it did not exercise GPU/NCCL workloads or 128-node
fan-out. Preparation and manifest distribution costs are outside NCCL init
timers and must be accounted for separately in job-startup comparisons.
