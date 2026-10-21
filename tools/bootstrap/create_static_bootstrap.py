#!/usr/bin/env python3
r"""Select ports and create a new static-bootstrap manifest.

For the simpler reserved-port-only schema-4 interface, use
prepare_nccl_bootstrap.py. This compatibility entrypoint retains older modes
and provides shared allocation, manifest and controller functions.

    python3 create_static_bootstrap.py 2 --allocation approved-nodes.json \
        --output static-bootstrap-next.json
    python3 create_static_bootstrap.py 2 --allocation approved-nodes.json \
        --output static-bootstrap-next.json --probe

Without --probe, validate and print the first-N plan only: no SSH and no output
file. With --probe, find four ports per node in 24000-24999 and publish a
manifest only after ALL selected nodes succeed. Existing files are never
overwritten. JSON diagnostics, including unsuccessful probes, go to stdout.
Add --hold-listeners --hold-seconds 600 for schema-2 prepared listeners:
the controller stays attached until ranks take their FDs or preparation fails.
Add --startup-concurrency NUM_NODES to start all helpers concurrently; the
default is one startup at a time, without stopping helpers already ready.
This mode emits JSON Lines (prepared, then handed_off/error), not one JSON object.

See tools/bootstrap/README.md for the required approved allocation JSON.
IP addresses must come from fresh NodeWarden/MRC tool observations, not an old
hostfile. This program neither queries a direct inventory API nor reserves
nodes. File contents and owner labels are not authentication; confirm the exact
selected nodes before --probe. The input snapshots must be at most five minutes
old, health PASS at most 90 minutes old, and reservations cover the bounded
probe sequence. These recorded checks cannot detect subsequent allocation
changes; requery before a real NCCL run.

The checker supplies the bounded SSH and temporary listener implementation.
Only EADDRINUSE is skipped. Any other failure aborts publication. Selected
sockets are closed after each node's scan in the default mode: the manifest
records point-in-time availability, NOT port reservations. No NCCL build,
benchmark, GPU readiness check or service change is performed.
"""

import argparse
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import secrets
import select
import shlex
import signal
import subprocess
import sys
import tempfile
import time
import uuid

import check_bootstrap_ports as checker


RANKS_PER_NODE = 4
PORT_START = 24000
PORT_END = 24999
SNAPSHOT_MAX_AGE = 300
HEALTH_MAX_AGE = 5400


def timestamp(value, label: str) -> datetime:
    checker.require(isinstance(value, str), f"missing {label}")
    parsed = datetime.fromisoformat(value)
    checker.require(parsed.utcoffset() is not None, f"{label} must include a timezone")
    return parsed


def validate_eligibility(node: dict, now: datetime, deadline: datetime) -> None:
    ip = node["ip"]
    checker.require(node.get("state") == "reserved", f"{ip}: not reserved")
    checker.require(isinstance(node.get("owner"), str) and bool(node["owner"].strip()),
                    f"{ip}: missing reservation owner")
    checker.require("note" in node and (node["note"] is None or isinstance(node["note"], str)),
                    f"{ip}: missing/invalid reservation note")
    checker.require(timestamp(node.get("until"), f"{ip} reservation expiry") > deadline,
                    f"{ip}: reservation does not cover the probe budget")
    checker.require(node.get("health") == "pass", f"{ip}: recorded health is not PASS")
    age = (now - timestamp(node.get("last_check_at"), f"{ip} health timestamp")).total_seconds()
    checker.require(0 <= age <= HEALTH_MAX_AGE, f"{ip}: recorded health is stale or in the future")
    checker.require(node.get("operations_eligible") is True and node.get("reachability") == "reachable",
                    f"{ip}: not eligible/reachable in the recorded inventory")


def validate_allocation(data, count: int, now: datetime, hold_seconds=0) -> tuple[list[dict], datetime]:
    checker.require(type(count) is int and count > 0, "NUM_NODES must be positive")
    checker.require(isinstance(data, dict), "allocation must be an object")
    checker.require(type(data.get("schema_version")) is int and data["schema_version"] == 1,
                    "expected allocation schema_version=1")
    checker.require(data.get("source") == "mrc-mcp", "allocation must identify its mrc-mcp source")
    for field in ("inventory_observed_at", "reservations_observed_at"):
        age = (now - timestamp(data.get(field), field)).total_seconds()
        checker.require(0 <= age <= SNAPSHOT_MAX_AGE, f"{field} is stale or in the future")
    warnings = data.get("warnings")
    checker.require(isinstance(warnings, list) and all(isinstance(value, str) for value in warnings),
                    "allocation must preserve source warnings as a list")
    nodes = data.get("nodes")
    checker.require(isinstance(nodes, list) and len(nodes) >= count,
                    "not enough nodes in the supplied allocation")
    approved = data.get("approved_ips")
    checker.require(isinstance(approved, list) and bool(approved), "missing approved_ips")
    approved_ips = [checker.ipv4(ip) for ip in approved]
    checker.require(len(set(approved_ips)) == len(approved_ips), "duplicate approved IPs")
    identities = []
    for node in nodes:
        checker.require(isinstance(node, dict), "allocation node must be an object")
        identities.append(checker.parse_identity({
            "ip": node.get("ip"), "hostname": node.get("hostname"),
            "interface": data.get("socket_ifname"),
        }))
    ips = [identity[0] for identity in identities]
    checker.require(len(set(ips)) == len(ips), "duplicate allocation IPs")
    checker.require(len({identity[1].casefold() for identity in identities}) == len(identities),
                    "duplicate allocation hostnames")
    checker.require(set(approved_ips) == set(ips), "approved_ips must match the supplied ordered node pool")
    placement = data.get("placement")
    checker.require(isinstance(placement, dict), "missing explicit placement")
    kind = placement.get("kind")
    checker.require(kind in ("same_rack", "same_row", "multi_row"),
                    "placement must be same_rack, same_row or multi_row")
    key = "rack" if kind == "same_rack" else "row_key"
    if kind == "multi_row":
        rows = placement.get("row_keys")
        checker.require(isinstance(rows, list) and len(rows) >= 2 and
                        all(isinstance(row, str) and bool(row.strip()) for row in rows),
                        "multi_row requires at least two explicit nonempty row keys")
        checker.require(len(set(rows)) == len(rows), "duplicate placement row keys")
        checker.require("row_key" not in placement and "rack" not in placement,
                        "multi_row cannot include single-row or rack constraints")
        allowed = rows
    else:
        checker.require(isinstance(placement.get(key), str) and bool(placement[key]), f"missing placement {key}")
        allowed = [placement[key]]
    selected = nodes[:count]
    deadline = now + timedelta(seconds=count * (checker.SSH_SECONDS + (20 if hold_seconds else 0)) + 60 + hold_seconds)
    for node in selected:
        for field in ("rack", "row_key"):
            checker.require(isinstance(node.get(field), str) and bool(node[field]),
                            f"{node['ip']}: incomplete topology")
        checker.require(node[key] in allowed, f"{node['ip']}: placement mismatch")
        validate_eligibility(node, now, deadline)
    return selected, deadline


def make_search(node: dict, interface: str, index: int, fixed_ports=None) -> checker.PortSearch:
    return checker.parse_search({
        "ip": node["ip"], "hostname": node["hostname"], "interface": interface,
        "first_rank": index * RANKS_PER_NODE, "count": RANKS_PER_NODE,
        "port_start": min(fixed_ports) if fixed_ports else PORT_START,
        "port_end": max(fixed_ports) if fixed_ports else PORT_END,
    })


def build_manifest(allocation: dict, selected: list[dict], reports: list[dict],
                   run_id: str, allocation_sha256: str, prepared=False, fixed_ports=None, strided_groups=False) -> dict:
    checker.require(len(selected) == len(reports) > 0, "incomplete node results")
    ranks = []
    for index, (node, report) in enumerate(zip(selected, reports)):
        search = make_search(node, allocation["socket_ifname"], index, fixed_ports)
        if prepared:
            checker.validate_held_ready(report, search, run_id, fixed_ports)
        else:
            checker.validate_selection_response(report, search, 0)
            checker.require(type(report.get("ssh_exit_code")) is int and report["ssh_exit_code"] == 0,
                            "missing successful SSH result")
        for local_rank, entry in enumerate(report["ports"]):
            ranks.append({
                "rank": entry["rank"], "local_rank": local_rank, "cuda_device": local_rank,
                "ip": node["ip"], "mrc_bootstrap_port": entry["port"],
            })
    bootstrap_magic = secrets.randbelow(2**64 - 1) + 1
    mrc_magic = secrets.randbelow(2**64 - 1) + 1
    while mrc_magic == bootstrap_magic:
        mrc_magic = secrets.randbelow(2**64 - 1) + 1
    data = {
        "schema_version": 2 if prepared else 1, "bootstrap_mode": "static_file_mrc", "run_id": run_id,
        "created_at": checker.utc_now(), "nranks": len(ranks), "ranks_per_node": RANKS_PER_NODE,
        "communicators": 1, "initialization": "fresh", "ring_order": "ascending_rank",
        "bootstrap_magic": f"0x{bootstrap_magic:016x}", "mrc_socket_magic": f"0x{mrc_magic:016x}",
        "network": {
            "plugin": "MRC", "oob_net_enable": True, "socket_ifname": allocation["socket_ifname"],
            "oob_net_ifname": None, "ib_subnet_aware_routing": False,
        },
        "ranks": ranks,
        "provenance": {
            "inventory_source": "NodeWarden via mrc-mcp Cluster Manager",
            "inventory_observed_at": allocation["inventory_observed_at"],
            "reservations_observed_at": allocation["reservations_observed_at"],
            "allocation_sha256": allocation_sha256, "placement": allocation["placement"],
            "warnings": allocation["warnings"],
            "nodes": [{
                "ip": node["ip"], "hostname": node["hostname"], "rack": node["rack"],
                "row_key": node["row_key"], "recorded_health": node["health"],
                "last_health_check_at": node["last_check_at"], "reservation_expires_at": node["until"],
            } for node in selected],
            "port_selection": {
                "range": [PORT_START, PORT_END], "ranks_per_node": RANKS_PER_NODE,
                "method": "ascending exact-IP bind/listen without socket reuse; skip only EADDRINUSE",
                "reports": reports,
            },
        },
        "readiness": {
            "runtime_support": "requires_patched_libraries", "port_availability": "available_at_probe_time",
            "ports_reserved": prepared, "staged_on_nodes": False, "execution_authorized": False,
        },
        "notes": [
            "Custom manifest; requires the patched NCCL and MRC libraries with NCCL_STATIC_BOOTSTRAP_FILE enabled.",
            "Every rank must read identical immutable contents; next peer is (rank + 1) modulo nranks.",
            "Root rendezvous and callback endpoints are absent because the static path bypasses those exchanges.",
            "Selected sockets have been closed. Recheck exact ports before running and fail on a real bind conflict.",
            "Only bootstrap-ring MRC listeners use these ports; later transport listeners retain normal allocation.",
            "Construct handles locally with zero initial stage and listener GIDs; do not replay raw private ABI bytes.",
            "Apply both per-run magic values consistently; they are identifiers, not authentication secrets.",
            "MRC connection setup still exchanges runtime metadata using TCP.",
            "Requery node reservations and health, verify GPU/process/software readiness, and approve a bounded run before execution.",
            "Four GPU ranks per node are planned, not verified by these port probes. Record file-distribution cost separately.",
            "Generate a new manifest for another run; this manifest does not authorize workloads.",
        ],
    }
    if prepared:
        data["listener_handoff"] = {
            "protocol": "scm_rights_v1",
            "nodes": [{"ip": node["ip"], "socket_path": report["socket_path"]}
                      for node, report in zip(selected, reports)],
        }
        data["readiness"]["port_availability"] = "held_by_live_helpers_at_publication"
        data["notes"] = [
            "Schema 2 requires the prepared-listener NCCL reader and ncclMrcBootstrapListen_v2 plugin extension.",
            "Helpers retain real exclusive TCP listeners, then transfer each FD exactly once to a same-UID local rank.",
            "Keep the preparation controller running until every handoff finishes; monitor its final status.",
            "Helper lifetime is bounded; manifest presence alone never proves that listeners are still ready.",
            "A failed or expired preparation leaves this manifest as audit evidence, not a usable port plan.",
            "No close/rebind, socket reuse, dynamic-port fallback, or fallback to schema-1 bootstrap is permitted.",
            "Remaining MRC TCP runtime-metadata exchanges and peer/proxy setup are unchanged.",
            "Distribute identical manifest bytes; retain current allocation, identity and execution gates.",
        ]
    if fixed_ports is not None:
        checker.require(prepared, "persistent listeners require live preparation")
        data.update(schema_version=3, communicators="multiple_same_ranks",
                    fixed_ports=checker.validate_fixed_ports(fixed_ports))
        data["listener_handoff"]["protocol"] = "persistent_scm_rights_v1"
        data["provenance"]["port_selection"].update(
            method="exact fixed-IP/port bind/listen without socket reuse or fallback", fixed_ports=fixed_ports,
            range=[min(fixed_ports), max(fixed_ports)])
        data["notes"] = [
            "Schema 3 requires the persistent NCCL reader and private MRC v3 extension.",
            "Helpers own four exclusive listeners for the bounded job lifetime, including between communicators.",
            "Fresh communicator IDs distinguish connections; every communicator uses the same GPUs/rank ordering.",
            "The helper consumes the MRC magic/type prefix and versioned routing header before connected-FD handoff.",
            "Send STOP on the attached controller's stdin after all communicator registrations have closed.",
            "EOF, cancellation, expiry and shutdown with live registrations are errors, not successful completion.",
            "Transferred connections remain rank-owned; helper shutdown does not terminate ranks.",
            "Other NCCL peer/proxy/transport ports and MRC runtime metadata exchange are unchanged.",
        ]
    if strided_groups:
        checker.require(fixed_ports is not None and prepared, "strided groups require persistent preparation")
        checker.require(all(report.get("strided_groups") is True for report in reports),
                        "helpers do not support strided groups")
        data.update(schema_version=4, communicators="strided_groups")
        data["listener_handoff"]["protocol"] = "persistent_scm_rights_v2"
        data["notes"][0] = "Schema 4 requires group-bound NCCL IDs and private MRC v4 endpoint routing."
        data["notes"][2] = "Each ID selects first/stride/count within this immutable four-endpoint-per-node pool."
    checker.parse_manifest(data)
    return data


def publish_manifest(path: Path, data: dict) -> None:
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         prefix=".static-bootstrap-", suffix=".tmp", delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(data, stream, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        # Linking a complete same-filesystem file is atomic and refuses overwrites.
        os.link(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink()


def read_helper_record(process, buffer, seconds):
    deadline = time.monotonic() + seconds
    while b"\n" not in buffer:
        remaining = deadline - time.monotonic()
        checker.require(remaining > 0, "timed out waiting for listener helper record")
        ready, _, _ = select.select([process.stdout], [], [], remaining)
        checker.require(bool(ready), "timed out waiting for listener helper record")
        chunk = os.read(process.stdout.fileno(), 4096)
        checker.require(bool(chunk), "listener helper closed stdout before its status record")
        buffer.extend(chunk)
        checker.require(len(buffer) <= 1024 * 1024, "listener helper record exceeds 1 MiB")
    line, _, rest = buffer.partition(b"\n")
    buffer[:] = rest
    record = checker.read_json(line)
    checker.require(isinstance(record, dict), "listener helper record must be an object")
    return record


def prepare_and_hold(allocation, selected, searches, run_id, allocation_sha256, output, lifetime,
                     startup_concurrency=1, fixed_ports=None, control_fd=None, strided_groups=False):
    workers = []
    result = {"outcome": "error", "run_id": run_id, "manifest": str(output.resolve()),
              "manifest_published": False, "nodes": [], "warnings": allocation["warnings"],
              "startup_concurrency": startup_concurrency, "persistent_listeners": fixed_ports is not None,
              "fixed_ports": fixed_ports, "strided_groups": strided_groups}
    status = 2
    previous = signal.getsignal(signal.SIGTERM)
    def interrupted(signum, frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    try:
        checker.require(type(startup_concurrency) is int and 1 <= startup_concurrency <= len(selected),
                        "startup concurrency must be between 1 and NUM_NODES")
        checker.require(len(searches) == len(selected), "one listener search is required per selected node")
        checker.require(not strided_groups or fixed_ports is not None, "strided groups require persistent listeners")
        if fixed_ports is not None:
            checker.validate_fixed_ports(fixed_ports)
            control_fd = sys.stdin.fileno() if control_fd is None else control_fd
        source = Path(checker.__file__).read_text()
        pending = []
        while len(workers) < len(selected) or pending:
            while len(workers) < len(selected) and len(pending) < startup_concurrency:
                node, target = selected[len(workers)], searches[len(workers)]
                validate_eligibility(node, datetime.now(timezone.utc),
                                     datetime.now(timezone.utc) + timedelta(seconds=lifetime + 60))
                spec = {"search": asdict(target), "run_id": run_id, "hold_seconds": lifetime}
                if fixed_ports is not None:
                    spec.update(fixed_ports=fixed_ports, nranks=len(selected) * 4,
                                previous_ip=selected[(len(workers) - 1) % len(selected)]["ip"])
                    if strided_groups:
                        spec.update(strided_groups=True, node_ips=[node["ip"] for node in selected])
                worker_mode = "--persistent-worker" if fixed_ports is not None else "--hold-worker"
                remote = shlex.join(["timeout", "--signal=TERM", "--kill-after=2s", f"{lifetime + 10}s",
                                     "python3", "-u", "-c", source, worker_mode, json.dumps(spec, separators=(",", ":"))])
                command = ["ssh", *checker.SSH_OPTIONS, f"azhpcuser@{target.ip}", remote]
                error = tempfile.TemporaryFile()
                started = time.monotonic()
                try:
                    process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=error)
                except OSError:
                    error.close()
                    raise
                worker = {"process": process, "error_file": error, "buffer": bytearray(), "target": target,
                          "started": started,
                          "record": {"ip": target.ip, "ssh_pid": process.pid,
                                     "worker_source_sha256": hashlib.sha256(source.encode()).hexdigest()}}
                workers.append(worker)
                pending.append(worker)
            now = time.monotonic()
            for worker in workers:
                checker.require(now - worker["started"] < lifetime,
                                "earlier listener lifetime is exhausted; manifest not published")
            remaining = min(worker["started"] + checker.SSH_SECONDS - now for worker in pending)
            checker.require(remaining > 0, "timed out waiting for listener helper readiness")
            buffered = [w["process"].stdout for w in workers if b"\n" in w["buffer"]]
            readable = buffered or select.select([w["process"].stdout for w in workers], [], [],
                                                 min(remaining, 1))[0]
            for worker in workers:
                process, buffer = worker["process"], worker["buffer"]
                if process.stdout not in readable:
                    continue
                # Read only available bytes so a partial record cannot block other nodes.
                if b"\n" not in buffer:
                    chunk = os.read(process.stdout.fileno(), 4096)
                    checker.require(bool(chunk), "listener helper closed stdout before publication")
                    buffer.extend(chunk)
                    checker.require(len(buffer) <= 1024 * 1024, "listener helper record exceeds 1 MiB")
                if b"\n" not in buffer:
                    continue
                report = read_helper_record(process, buffer, 0)
                if worker not in pending:
                    worker["record"]["completion"] = report
                    raise ValueError("listener helper stopped before publication")
                if report.get("outcome") == "prepared":
                    worker["record"]["ready"] = report
                else:
                    worker["record"]["completion"] = report
                checker.require(time.monotonic() - worker["started"] < checker.SSH_SECONDS,
                                "timed out waiting for listener helper readiness")
                checker.require(b"REMOTE HOST IDENTIFICATION HAS CHANGED" not in
                                os.pread(worker["error_file"].fileno(), 65536, 0),
                                "SSH host identity changed; listener preparation stopped")
                checker.validate_held_ready(report, worker["target"], run_id, fixed_ports)
                checker.require(report["hold_seconds"] == lifetime, "helper lifetime differs from requested lifetime")
                checker.require(process.poll() is None, "listener helper exited before publication")
                worker["record"]["startup_ready_seconds"] = time.monotonic() - worker["started"]
                pending.remove(worker)
        for worker in workers:
            checker.require(worker["process"].poll() is None, "listener helper stopped during preparation")
            checker.require(time.monotonic() - worker["started"] < lifetime,
                            "earlier listener lifetime is exhausted; manifest not published")
            readable, _, _ = select.select([worker["process"].stdout], [], [], 0)
            if readable or b"\n" in worker["buffer"]:
                worker["record"]["completion"] = read_helper_record(worker["process"], worker["buffer"], 1)
                raise ValueError("listener helper stopped before publication")
        for node in selected:
            validate_eligibility(node, datetime.now(timezone.utc),
                                 datetime.now(timezone.utc) + timedelta(seconds=lifetime + 60))
        manifest = build_manifest(allocation, selected, [w["record"]["ready"] for w in workers],
                                  run_id, allocation_sha256, prepared=True, fixed_ports=fixed_ports,
                                  strided_groups=strided_groups)
        publish_manifest(output, manifest)
        result["manifest_published"] = True
        print(json.dumps({**result, "outcome": "prepared", "nodes": [w["record"] for w in workers],
                          "instruction": "Keep this controller attached; launch only under a separately approved run plan."}),
              flush=True)
        pending = list(workers)
        deadline = time.monotonic() + lifetime + 15
        stopped = False
        control_buffer = bytearray()
        while pending:
            remaining = deadline - time.monotonic()
            checker.require(remaining > 0, "preparation controller deadline exceeded")
            streams = [w["process"].stdout for w in pending]
            if fixed_ports is not None and not stopped:
                streams.append(control_fd)
            readable, _, _ = select.select(streams, [], [], min(remaining, 1))
            if fixed_ports is not None and not stopped and control_fd in readable:
                chunk = os.read(control_fd, 64)
                checker.require(bool(chunk), "persistent controller stdin closed without STOP")
                control_buffer.extend(chunk)
                checker.require(len(control_buffer) <= 5, "invalid controller shutdown command")
                if b"\n" in control_buffer:
                    checker.require(control_buffer == b"STOP\n", "expected STOP")
                    for worker in workers:
                        worker["process"].stdin.write(b"STOP\n")
                        worker["process"].stdin.flush()
                    stopped = True
            for worker in list(pending):
                if worker["process"].stdout not in readable:
                    continue
                completion = read_helper_record(worker["process"], worker["buffer"], min(remaining, 5))
                worker["record"]["completion"] = completion
                expected = list(range(worker["target"].first_rank, worker["target"].first_rank + 4))
                successful = (stopped and completion.get("outcome") == "stopped") if fixed_ports is not None else (
                    completion.get("outcome") == "handed_off" and sorted(completion.get("claimed_ranks", [])) == expected)
                checker.require(completion.get("run_id") == run_id and completion.get("ip") == worker["target"].ip and successful and
                                completion.get("owned_descriptors_closed") is True and
                                completion.get("helper_directory_removed") is True,
                                "listener handoff did not complete successfully")
                checker.require(worker["process"].wait(timeout=5) == 0, "listener helper SSH exited unsuccessfully")
                pending.remove(worker)
        status = 0
    except (OSError, ValueError, subprocess.TimeoutExpired, KeyboardInterrupt) as error:
        result["error"] = str(error) or "preparation interrupted"
        print(f"ERROR: {result['error']}", file=sys.stderr, flush=True)
    finally:
        for worker in workers:
            worker["process"].stdin.close()
        for worker in workers:
            process, record = worker["process"], worker["record"]
            try:
                cleanup_deadline = time.monotonic() + 5
                while "completion" not in record:
                    report = read_helper_record(process, worker["buffer"], cleanup_deadline - time.monotonic())
                    if report.get("outcome") == "prepared":
                        checker.require("ready" not in record, "duplicate helper readiness during cleanup")
                        record["ready"] = report
                    else:
                        record["completion"] = report
                record["ssh_exit_code"] = process.wait(timeout=5)
            except (OSError, ValueError, subprocess.TimeoutExpired) as error:
                record["cleanup_error"] = str(error)
                record["remote_cleanup_confirmed"] = False
                status = 2
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
            completion = record.get("completion", {})
            record["remote_cleanup_confirmed"] = (
                "cleanup_error" not in record and completion.get("run_id") == run_id and
                completion.get("ip") == worker["target"].ip and completion.get("owned_descriptors_closed") is True and
                completion.get("helper_directory_removed") is True)
            if not record["remote_cleanup_confirmed"]:
                status = 2
            worker["error_file"].seek(0)
            record["ssh_stderr"] = worker["error_file"].read().decode(errors="replace")
            worker["error_file"].close()
            process.stdout.close()
            if "REMOTE HOST IDENTIFICATION HAS CHANGED" in record["ssh_stderr"]:
                result["error"] = "SSH host identity changed"
                status = 2
        signal.signal(signal.SIGTERM, previous)
        success = "stopped" if fixed_ports is not None else "handed_off"
        result.update(outcome=success if status == 0 else "error", nodes=[w["record"] for w in workers])
        print(json.dumps(result), flush=True)
    return status


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("num_nodes", type=int, metavar="NUM_NODES")
    parser.add_argument("--allocation", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True, help="new manifest path; never overwritten")
    parser.add_argument("--probe", action="store_true", help="authorize port selection and manifest publication")
    parser.add_argument("--hold-listeners", action="store_true",
                        help="prepare real listeners, publish schema 2, and stay attached through FD handoff")
    parser.add_argument("--hold-seconds", type=int, help="required with --hold-listeners; finite lifetime 1..3600")
    parser.add_argument("--startup-concurrency", type=int,
                        help="with --hold-listeners only: concurrent startups, 1..NUM_NODES (default: 1)")
    parser.add_argument("--persistent-listeners", action="store_true",
                        help="opt-in schema 3: keep fixed listeners across same-rank communicators until stdin STOP")
    parser.add_argument("--fixed-ports", type=int, nargs=4, metavar="PORT",
                        help="four exact ports per node, required only with --persistent-listeners")
    parser.add_argument("--strided-groups", action="store_true",
                        help="opt-in schema 4: communicator IDs select first/stride/count from the job endpoint pool")
    args = parser.parse_args(argv)
    try:
        checker.require(args.output.parent.is_dir(), "output directory does not exist")
        checker.require(not args.output.exists() and not args.output.is_symlink(), "output path already exists")
        raw = args.allocation.read_bytes()
        allocation = checker.read_json(raw)
        checker.require((args.hold_listeners and type(args.hold_seconds) is int and 1 <= args.hold_seconds <= 3600) or
                        (not args.hold_listeners and args.hold_seconds is None),
                        "--hold-listeners requires --hold-seconds in 1..3600; neither flag applies alone")
        checker.require(args.startup_concurrency is None or
                        (args.hold_listeners and 1 <= args.startup_concurrency <= args.num_nodes),
                        "--startup-concurrency requires --hold-listeners and a value in 1..NUM_NODES")
        checker.require((args.persistent_listeners and args.hold_listeners and args.fixed_ports is not None) or
                        (not args.persistent_listeners and args.fixed_ports is None),
                        "--persistent-listeners requires --hold-listeners and --fixed-ports")
        if args.persistent_listeners:
            checker.validate_fixed_ports(args.fixed_ports)
        checker.require(not args.strided_groups or args.persistent_listeners,
                        "--strided-groups requires --persistent-listeners")
        selected, deadline = validate_allocation(allocation, args.num_nodes, datetime.now(timezone.utc),
                                                 args.hold_seconds or 0)
        searches = [make_search(node, allocation["socket_ifname"], index, args.fixed_ports)
                    for index, node in enumerate(selected)]
        source = Path(checker.__file__).read_text() if args.probe else None
    except (OSError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 2
    run_id = str(uuid.uuid4())
    allocation_sha256 = hashlib.sha256(raw).hexdigest()
    result = {
        "outcome": "not_probed", "run_id": run_id, "num_nodes": args.num_nodes,
        "nranks": args.num_nodes * RANKS_PER_NODE, "output": str(args.output.resolve()),
        "manifest_published": False, "ports_reserved": False, "allocation_sha256": allocation_sha256,
        "probe_deadline": deadline.isoformat(), "plan": [asdict(search) for search in searches],
        "nodes": [], "warnings": allocation["warnings"],
        "hold_listeners": args.hold_listeners, "hold_seconds": args.hold_seconds,
        "startup_concurrency": (args.startup_concurrency or 1) if args.hold_listeners else None,
        "persistent_listeners": args.persistent_listeners, "fixed_ports": args.fixed_ports,
        "strided_groups": args.strided_groups,
    }
    if not args.probe:
        print(json.dumps(result, indent=2))
        return 0
    if args.hold_listeners:
        return prepare_and_hold(allocation, selected, searches, run_id, allocation_sha256,
                                args.output, args.hold_seconds, result["startup_concurrency"], args.fixed_ports,
                                strided_groups=args.strided_groups)
    status = 2
    try:
        for node, search in zip(selected, searches):
            now = datetime.now(timezone.utc)
            checker.require((deadline - now).total_seconds() >= checker.SSH_SECONDS,
                            "probe sequence deadline reached")
            validate_eligibility(node, now, deadline)
            report = checker.probe_remote(search, source)
            result["nodes"].append(report)
            if report["outcome"] != "available":
                result["outcome"] = report["outcome"]
                status = 1 if report["outcome"] == "unavailable" else 2
                break
        else:
            now = datetime.now(timezone.utc)
            checker.require(now < deadline, "probe sequence deadline reached before publication")
            for node in selected:
                validate_eligibility(node, now, deadline)
            data = build_manifest(allocation, selected, result["nodes"], run_id, allocation_sha256)
            publish_manifest(args.output, data)
            result.update(outcome="created", manifest_published=True)
            status = 0
    except (OSError, ValueError) as error:
        result.update(outcome="error", error=str(error))
        print(f"ERROR: {error}", file=sys.stderr)
    for skipped in selected[len(result["nodes"]):]:
        result["nodes"].append({"ip": skipped["ip"], "outcome": "not_probed",
                                "reason": "stopped before publication"})
    print(json.dumps(result, indent=2))
    return status


if __name__ == "__main__":
    sys.exit(main())
