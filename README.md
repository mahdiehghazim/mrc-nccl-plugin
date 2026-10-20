# mrc-nccl-plugin

An external NCCL network plugin using MRC (Multi-Path Reliable Connection) as transport. This rebased C++ branch builds the NCCL NET v12 ABI and reports the network name `MRC`. It requires a NCCL runtime that supports v12; the legacy C sources are not part of this build.


## Requirements

* MRC
* CUDA
* OFED
* NCCL with NET v12 support (runtime)
* A C++17 compiler, Make, and Python 3 (build)

## Build

```bash
make MRC_HOME=/path/to/mrc CUDA_HOME=/path/to/cuda
```
Debug build
```bash
make MRC_HOME=/path/to/mrc CUDA_HOME=/path/to/cuda DEBUG=1
```

To supply `nccl.h` from a custom NCCL installation, add `NCCL_HOME=/path/to/nccl/build` to the make command. `nccl.h` will be included from `$(NCCL_HOME)/include`.

`libnccl-net-mrc.so` will be generated after building. `mrc` is the suffix of the plugin library.

## Run (NCCL)

NCCL loads external plugins via the `NCCL_NET_PLUGIN` environment variable. If `NCCL_NET_PLUGIN` is set as `libnccl-net-mrc.so` or just as the suffix `mrc`, add the folder containing the plugin library to your `LD_LIBRARY_PATH`. Instead, `NCCL_NET_PLUGIN` can be set to the absolute path, for e.g., `/path/to/libnccl-net-mrc.so`.

With `NCCL_DEBUG=INFO`, verify `Loaded net plugin MRC (v12)`, `Using network MRC`. For the version of mrc-nccl-plugin being used, look for `Initializing MRC plugin version` string. The plugin version and the git version will follow it. A `NET/Plugin` load error followed by `Using network IB` means NCCL fell back to its built-in transport; subsequent IB CQE errors do not establish a failure in the MRC data path. Setting `NCCL_NET=MRC` (and forwarding it to every rank) makes such fallback fail explicitly instead. 

## Experimental static bootstrap extension

This local branch exports `ncclMrcBootstrapListen_v1` in addition to the unchanged
NET v12 interface. The matching patched NCCL library reads the manifest selected
by `NCCL_STATIC_BOOTSTRAP_FILE` and passes validated logical endpoints and magic
to this extension. The plugin does not parse JSON or depend on raw captured
handle bytes.

Only the bootstrap-ring listener binds a manifest-assigned port, without
`SO_REUSEADDR` or `SO_REUSEPORT`. The next-rank handle is constructed using the
plugin's own types, with zero initial stage and GIDs. The selected local IPv4
interface must match, and `NCCL_IB_SUBNET_AWARE_ROUTING` must be zero. Bind errors
are returned; there is no port sharing or fallback.

Ordinary `listen()` calls retain their normal ephemeral ports and default magic.
The extension does not remove the TCP setup used by MRC `connect()`/`accept()` to
exchange runtime metadata. The companion NCCL patches and Python manifest/listener
tools are not bundled in this plugin repository. The evaluation workspace's
`static-bootstrap-tools.txt` documents the coordinated configuration and current
limits. For schema 1, private
full builds and three normal/static pairs passed on two four-GPU GB200 nodes with eight-rank
4 KiB AllReduce correctness. Static mode confirmed exclusive manifest listeners,
but peer-listener readiness races triggered TCP retry backoff and no startup
speedup was observed. Larger-scale schema-1 and production validation remain pending.

The additional `ncclMrcBootstrapListen_v2` export consumes an already-listening
socket from a same-UID, node-local helper using `SCM_RIGHTS`. It verifies the
private Unix endpoint, peer UID, TCP listener endpoint/state and exclusive
binding before acknowledged adoption, with bounded handoff waits. It never
closes and rebinds the port or falls back to v1. Use the updated NCCL reader and
schema-2 manifest generated with `--hold-listeners --hold-seconds N`; keep the
preparation controller attached until all handoffs finish. A private rebuild
and three normal/prepared pairs passed on the same eight GPUs, confirming FD
adoption on every prepared rank and zero connection retries. Median full init
was 1473.762 ms normal versus 1505.349 ms prepared (+2.14%), so this small sample
did not show a full-init speedup despite removing the earlier refusal retries.
See the tools document for separate preparation costs and complete results.

A four-node/sixteen-rank follow-up passed another three pairs using the same
NCCL/MRC libraries, with 16/16 correctness, adoption on every prepared rank and
zero connection retries. Only the benchmark's layout guard was rebuilt after
an initial pre-init abort, which is excluded from the results. Median full init
was 1552.090 ms normal versus 1557.784 ms prepared (+0.37%), again without a
demonstrated full-init speedup. Preparation/distribution were measured
separately.

A 90-node/360-rank, four-row follow-up completed two pairs with the same native
NCCL/MRC libraries. All four runs passed correctness on every rank, both
prepared runs confirmed all 360 listener adoptions, and no connection retries
were logged. Median full init was 2549.5 ms normal versus 1831.9 ms prepared
(-28.14%). The five-pair plan stopped after unexpected reservation loss, at the
user's request; only two pairs are reported. External listener preparation
took a median 52.854 seconds, outside the NCCL timers. These observations are
not an end-to-end job-startup claim or a statistically established scaling
result. See the tools document for complete partial results and cleanup
evidence. Production validation remains pending.

### Persistent job listeners (schema 3)

The opt-in `ncclMrcBootstrapListen_v3` registers a communicator with a persistent
same-UID helper rather than binding or adopting a listener. The helper owns four
exact, exclusive TCP ports per node until explicit job shutdown. Concurrent and
sequential communicators use fresh tokens, with unchanged GPUs and rank order.
The connector sends the normal magic/type prefix followed by a 28-byte `MRC3`
job/source/destination header; the helper validates and consumes exactly those
40 bytes and transfers the connected FD using acknowledged `SCM_RIGHTS`.
The receiver starts directly at the normal MRC metadata exchange. Each
communicator still owns separate TCP/MRC resources; the helper is not a relay.

`closeListen()` unregisters only that communicator. The additional private
`ncclMrcBootstrapCancelConnect_v3` releases a pending outgoing connection on
bootstrap failure/abort. NET v12 is unchanged, and normal/schema-1/schema-2 paths
remain separate. The opaque handle still fits the 128-byte NET limit; deploy
matching builds across all ranks.

Use the generator's `--persistent-listeners --fixed-ports P0 P1 P2 P3` together
with `--hold-listeners --hold-seconds N`. Keep its stdin open; send `STOP` only
after communicator/workload cleanup. See the tools document for bounded queues,
deadlines, fresh IDs, early/interleaved arrivals and explicit error handling.
CPU socket/FD and extracted-native contract tests cover this implementation;
private full library builds and nine normal/prepared/persistent runs also passed
on eight nodes (32 GB200 GPUs) on 2026-10-19. Each of the three persistent jobs
completed 128 connected-FD handoffs with zero rejected requests, preserving all
four listener inodes per node across four communicators with overlapping
lifetimes. First init essentially matched prepared mode and was 4.7% lower than
normal; later init was 6-7% lower than normal. The matched
preparation-through-launcher-return envelope was 4.0% longer than normal, not an
end-to-end speedup. These are three fixed-order samples with sequential init
calls, not statistical or production validation. Concurrent initialization
progress, GPU failure injection and long-job capacity remain untested. See the
tools document for full results, preparation costs and timer boundaries.

### Strided job groups (schema 4)

`ncclMrcBootstrapListen_v4` supports fresh, variable-size positive-stride groups
inside the immutable four-endpoint-per-node job pool. Communicator ranks map to
stable endpoints through `first + stride*rank`; ports and helpers remain fixed.
The matching NCCL API `ncclGetUniqueIdForBootstrapGroup` puts the descriptor in
the distributed ID and binds the routing token to it. MRC4 headers route by
endpoint, and REG4 supplies the group for predecessor validation before
FD4/ACK4/DONE4 handoff. The 40-byte routing prefix and 128-byte NET handle limit
are preserved; NET v12 is unchanged. `ncclMrcBootstrapCancelConnect_v3` also
accepts v4 handles. Schemas 1-3 remain separate and unsupported versions fail.

Enable `--strided-groups` during persistent preparation. Full/subset overlap,
cross-node first-rank groups, offset/singleton groups, native routing, cleanup
and rejection paths have CPU/loopback coverage. Private full builds and six
normal/persistent GPU jobs passed on 64 nodes on 2026-10-20, with full A256 and
node-first B64 communicators alive together. Each persistent job completed
320 connected-FD handoffs with zero rejected requests and 320 checkpoints
confirming the same four listening sockets per node across both groups.
Median native bootstrap time was 40.5% lower for A and 48.5% lower for B,
but preparation-through-launcher-return was 2.0% longer, not an end-to-end
speedup. Three fixed-order pairs with sequential init calls do not establish
statistical significance, concurrent-init progress, GPU failure handling or
long-job capacity. These schema-4 observations are separate from the earlier
schema-3 results. See the tools document for usage, measured ranges and limits.
