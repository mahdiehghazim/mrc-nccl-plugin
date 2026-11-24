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

This experimental branch also builds `libnccl-init-trace.so` beside the plugin.
Keep both libraries together when deploying. The shared helper provides the
process-local early-connection handoff registry and optional initialization
profiling; it is required even when profiling is disabled. Matched NCCL builds
must use the same helper instance, not a separately compiled private registry.

## Experimental early TCPStore bootstrap

This branch exposes a versioned host-bootstrap endpoint API in
`support/src/early_mrc.h`:

| API suffix (`ncclMrcEarlyBootstrap...`) | Purpose |
|---|---|
| `DescriptorSizeV1` | Size of the versioned binary descriptor |
| `PrepareV1` | Allocate live sender/receiver endpoints, create/INIT QPs, register control buffers, and return local metadata |
| `FinishV1` | Validate predecessor/successor descriptors and finish the existing QPs, including receive posting |
| `BindV1` | Publish the ready endpoints into the shared, single-consumption registry |
| `CloseV1` | Release unconsumed endpoints, or close the preparation session after NCCL adopts them |

The caller must close even a partially prepared session after an error.
Prepare returns metadata for the actual live resources, not temporary endpoints.
Finish consumes the predecessor's sender metadata and successor's receiver
metadata without socket metadata exchange. The legacy connect/accept path
remains in place for ordinary calls.

The plugin does not implement TCPStore itself. The application must publish
each rank's descriptor and optional root unique ID, fetch neighbor descriptors
and root IDs, call Finish, exchange neighbor readiness, and then Bind before
blocking native communicator creation. Use fresh store keys for every
communicator instance and generation.

**Required companion changes (not included here):** a matched NCCL core that
generates marked `MRCEARLY` unique IDs and adopts connections through
`nitEarlyBootstrapTake`, plus the YOLO `early_mrc_bootstrap` coordinator.
Stock NCCL can use the normal plugin path, but cannot activate this early path.
This branch does not modify NCCL or YOLO.

The initial early path requires at least two ranks and exactly one physical MRC
device per rank. Select it with `NCCL_OOB_NET_ENABLE=1` and an exact
`NCCL_OOB_NET_IFNAME`, for example `=mlx5_0_dma`. Peers must use the same ABI,
QP count, traffic classes/SLs, matching scheme, preposting and inline settings.
Forced OOO RQ, device fusion, ECE, resiliency, subnet overrides and remote-speed
updates are unsupported. The application coordinator must also reject
`NCCL_COMM_ID`. These prepared connections carry registered host bootstrap
buffers, not the normal data transport.

This removes only bootstrap-ring endpoint establishment and its socket
metadata handshake. Proxy/P2P sockets, RAS and later NCCL AllGathers remain.
Include preparation, metadata exchange and readiness in end-to-end startup
measurements; a shorter native NCCL timer alone is not a performance gain.

### Tests and profiling

```bash
make -j4 NCCL_HOME=/path/to/nccl/build
make test NCCL_HOME=/path/to/nccl/build
```

The fake-provider tests execute the real connection implementation without GPU
work or network listeners. They check live-QP reuse, peer metadata, ownership,
no socket handshake, and cleanup at each of 30 injected provider failures in
both default and inline/prepost modes. Registry tests cover communicator
identity, one-time consumption and cancellation.

Profiling is opt-in: set `NCCL_INIT_TRACE_DIR` for native/provider spans,
`NCCL_IB_CONNECT_PROFILE_DIR` for sender phase/operation profiles, or
`NCCL_BOOTSTRAP_PROFILE_DIR` for the associated AllGather profiling helper.
Use existing writable node-local directories, unset these variables to disable
recording, and keep output outside the source tree. Sender elapsed time can
include waiting between Prepare and Finish; nested provider timers are not
additive.

## Run (NCCL)

NCCL loads external plugins via the `NCCL_NET_PLUGIN` environment variable. If `NCCL_NET_PLUGIN` is set as `libnccl-net-mrc.so` or just as the suffix `mrc`, add the folder containing the plugin library to your `LD_LIBRARY_PATH`. Instead, `NCCL_NET_PLUGIN` can be set to the absolute path, for e.g., `/path/to/libnccl-net-mrc.so`.

With `NCCL_DEBUG=INFO`, verify `Loaded net plugin MRC (v12)`, `Using network MRC`. For the version of mrc-nccl-plugin being used, look for `Initializing MRC plugin version` string. The plugin version and the git version will follow it. A `NET/Plugin` load error followed by `Using network IB` means NCCL fell back to its built-in transport; subsequent IB CQE errors do not establish a failure in the MRC data path. Setting `NCCL_NET=MRC` (and forwarding it to every rank) makes such fallback fail explicitly instead. 
