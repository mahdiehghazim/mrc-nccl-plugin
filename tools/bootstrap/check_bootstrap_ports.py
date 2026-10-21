#!/usr/bin/env python3
"""Validate a static-bootstrap port plan; contact nodes only with --probe.

Examples:
    python3 check_bootstrap_ports.py static-bootstrap.json
    python3 check_bootstrap_ports.py static-bootstrap.json --probe > port-report.json

Before --probe, confirm the exact allocation, identities, current reservations,
and fresh health through NodeWarden/MRC tools and obtain probe authorization.
This program does not perform those checks or launch NCCL. A saved manifest is
not proof of current ownership, health, or readiness.

The internal --hold-worker and --persistent-worker modes are used by the
preparation controllers. prepare_nccl_bootstrap.py is the reserved-port-only
entrypoint; create_static_bootstrap.py retains the older modes. Prepared workers
transfer listening FDs once; persistent workers retain listeners and transfer
connected FDs to same-UID ranks over a private Unix socket. Controller EOF,
SIGTERM, failure or expiry closes helper-owned descriptors. --probe accepts only
schema 1: schemas 2-4 already have live listeners.

Probes run sequentially as azhpcuser over SSH. Host-key checking is disabled to
match the existing benchmark harness; this weakens host authentication.
The remote node needs Python 3.10+, iproute2's `ip`, and GNU `timeout`.
Each remote helper is limited to 10 seconds plus 2 seconds kill grace; the
local SSH deadline is 25 seconds per node. Nothing is staged remotely.

Every requested port is bound on the exact interface IP and listened on without
SO_REUSEADDR/SO_REUSEPORT. Successful sockets are held until that node's checks
finish, then all owned sockets are closed. No connections are accepted, no
existing processes are stopped, and no alternative ports are selected.

Availability is only a point-in-time, conservative local bind/listen result,
not a reservation, a firewall/reachability test, or a guarantee of a later bind.
The manifest is never modified. JSON is written to stdout; diagnostics go to
stderr. Exit 0 means validation succeeded (without --probe) or all probes
succeeded; exit 1 means a port was unavailable; exit 2 means invalid input or
an infrastructure/protocol error. Always inspect `outcome`: `not_probed` is
not an availability result. Infrastructure errors stop further node probes.
"""

import argparse
import array
from contextlib import ExitStack
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import errno
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import select
import selectors
import signal
import shlex
import socket
import struct
import subprocess
import sys
import tempfile
import time
import uuid


PROBE_SECONDS = 10
SSH_SECONDS = 25
SSH_OPTIONS = [
    "-o", "BatchMode=yes",
    "-o", "StrictHostKeyChecking=no",
    "-o", "ConnectTimeout=8",
    "-o", "ServerAliveInterval=5",
    "-o", "ServerAliveCountMax=2",
]


@dataclass(frozen=True)
class Port:
    rank: int
    port: int


@dataclass(frozen=True)
class Node:
    ip: str
    hostname: str
    interface: str
    ports: tuple[Port, ...]


@dataclass(frozen=True)
class PortSearch:
    ip: str
    hostname: str
    interface: str
    first_rank: int
    count: int
    port_start: int
    port_end: int


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def unique_keys(pairs: list) -> dict:
    result = {}
    for key, value in pairs:
        require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def reject_constant(value: str) -> None:
    raise ValueError(f"invalid JSON constant: {value}")


def read_json(text: str | bytes):
    return json.loads(text, object_pairs_hook=unique_keys, parse_constant=reject_constant)


def ipv4(value) -> str:
    require(isinstance(value, str), "IP address must be a string")
    address = ipaddress.IPv4Address(value)
    require(not (address.is_unspecified or address.is_multicast or int(address) == 0xFFFFFFFF),
            f"IP must identify a specific unicast endpoint: {value}")
    return str(address)


def parse_identity(data) -> tuple[str, str, str]:
    require(isinstance(data, dict), "node must be an object")
    ip = ipv4(data.get("ip"))
    hostname = data.get("hostname")
    require(isinstance(hostname, str) and
            re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,252}", hostname) is not None,
            f"invalid expected hostname for {ip}")
    interface = data.get("interface")
    require(isinstance(interface, str) and
            re.fullmatch(r"[A-Za-z0-9_.:-]{1,15}", interface) is not None,
            f"invalid interface for {ip}")
    return ip, hostname, interface


def parse_node(data) -> Node:
    ip, hostname, interface = parse_identity(data)
    ports = data.get("ports")
    require(isinstance(ports, list) and bool(ports), f"missing ports for {ip}")
    parsed = []
    for entry in ports:
        require(isinstance(entry, dict), "port entry must be an object")
        rank, port = entry.get("rank"), entry.get("port")
        require(type(rank) is int and rank >= 0, f"invalid rank for {ip}")
        require(type(port) is int and 1 <= port <= 65535, f"invalid port for rank {rank}: {port}")
        parsed.append(Port(rank, port))
    require(len({entry.rank for entry in parsed}) == len(parsed), f"duplicate ranks on {ip}")
    require(len({entry.port for entry in parsed}) == len(parsed), f"duplicate endpoint on {ip}")
    return Node(ip, hostname, interface, tuple(parsed))


def parse_search(data) -> PortSearch:
    ip, hostname, interface = parse_identity(data)
    first, count = data.get("first_rank"), data.get("count")
    start, end = data.get("port_start"), data.get("port_end")
    require(type(first) is int and first >= 0, "invalid first_rank")
    require(type(count) is int and count > 0, "invalid requested port count")
    require(type(start) is int and type(end) is int and 1 <= start <= end <= 65535,
            "invalid port search range")
    require(count <= end - start + 1, "port search range is smaller than requested count")
    return PortSearch(ip, hostname, interface, first, count, start, end)


def parse_manifest(data) -> tuple[str, list[Node]]:
    require(isinstance(data, dict), "manifest must be an object")
    require(type(data.get("schema_version")) is int and data["schema_version"] in (1, 2, 3, 4),
            "expected schema_version=1, 2, 3 or 4")
    require(data.get("bootstrap_mode") == "static_file_mrc", "unsupported bootstrap_mode")
    run_id = data.get("run_id")
    require(isinstance(run_id, str), "missing run_id")
    require(str(uuid.UUID(run_id)) == run_id, "run_id must be a canonical UUID")
    nranks = data.get("nranks")
    require(type(nranks) is int and nranks > 0, "nranks must be a positive integer")
    rows = data.get("ranks")
    require(isinstance(rows, list) and len(rows) == nranks, "rank count differs from nranks")
    network = data.get("network")
    require(isinstance(network, dict), "missing network settings")
    require(network.get("plugin") == "MRC" and network.get("oob_net_enable") is True,
            "expected MRC with OOB enabled")
    provenance = data.get("provenance")
    require(isinstance(provenance, dict), "missing node provenance")
    identities = provenance.get("nodes")
    require(isinstance(identities, list) and bool(identities), "missing node identities")
    hosts = {}
    for entry in identities:
        require(isinstance(entry, dict), "node identity must be an object")
        ip = ipv4(entry.get("ip"))
        require(ip not in hosts, f"duplicate node identity: {ip}")
        hosts[ip] = entry.get("hostname")
    grouped = {}
    for rank, row in enumerate(rows):
        require(isinstance(row, dict), "rank entry must be an object")
        require(type(row.get("rank")) is int and row["rank"] == rank,
                "ranks must be contiguous and ordered from zero")
        ip = ipv4(row.get("ip"))
        require(ip in hosts, f"no recorded node identity for {ip}")
        grouped.setdefault(ip, []).append({"rank": rank, "port": row.get("mrc_bootstrap_port")})
    require(set(grouped) == set(hosts), "node identities do not match rank IPs")
    nodes = [parse_node({"ip": ip, "hostname": hosts[ip], "interface": network.get("socket_ifname"),
                         "ports": ports}) for ip, ports in grouped.items()]
    require(len({node.hostname.casefold() for node in nodes}) == len(nodes),
            "different IPs map to the same hostname")
    if data["schema_version"] in (2, 3, 4):
        handoff = data.get("listener_handoff")
        protocol = {2: "scm_rights_v1", 3: "persistent_scm_rights_v1", 4: "persistent_scm_rights_v2"}[data["schema_version"]]
        require(isinstance(handoff, dict) and handoff.get("protocol") == protocol,
                "missing supported listener handoff protocol")
        entries = handoff.get("nodes")
        require(isinstance(entries, list) and len(entries) == len(nodes), "handoff node count mismatch")
        for node, entry in zip(nodes, entries):
            require(isinstance(entry, dict) and entry.get("ip") == node.ip, "handoff node ordering mismatch")
            validate_handoff_path(entry.get("socket_path"), run_id)
        if data["schema_version"] >= 3:
            require(data.get("communicators") == ("strided_groups" if data["schema_version"] == 4 else "multiple_same_ranks") and
                    data.get("initialization") == "fresh" and data.get("ring_order") == "ascending_rank",
                    "persistent bootstrap requires fresh communicators with the same ordered ranks")
            require(data.get("ranks_per_node") == 4 and nranks <= 65536 and nranks == 4 * len(nodes),
                    "persistent bootstrap requires four ranks/node")
            fixed = validate_fixed_ports(data.get("fixed_ports"))
            for index, node in enumerate(nodes):
                require([p.rank for p in node.ports] == list(range(index * 4, index * 4 + 4)) and
                        [p.port for p in node.ports] == fixed, "fixed ports or rank ordering differ")
            for rank, row in enumerate(rows):
                require(type(row.get("local_rank")) is int and type(row.get("cuda_device")) is int and
                        row["local_rank"] == rank % 4 and row["cuda_device"] == rank % 4,
                        "persistent local GPU mapping differs")
    else:
        require("listener_handoff" not in data, "prepared listeners require schema_version=2")
    return run_id, nodes


def validate_handoff_path(path, run_id):
    require(isinstance(path, str) and re.fullmatch(
        r"/tmp/nccl-bootstrap\." + re.escape(run_id) + r"\.[a-z0-9_]{8}/handoff\.sock", path) is not None,
        "invalid private listener handoff path")


def validate_fixed_ports(ports):
    require(isinstance(ports, list) and len(ports) == 4 and
            all(type(port) is int and 1 <= port <= 65535 for port in ports) and
            len(set(ports)) == 4, "four distinct fixed TCP ports in 1..65535 are required")
    return ports


def group_predecessor(first, stride, count, endpoint, pool_size):
    require(all(type(v) is int for v in (first, stride, count)) and first >= 0 and
            1 <= stride <= 65536 and 1 <= count <= 65536 and first + stride * (count - 1) < pool_size,
            "strided group is outside endpoint pool")
    require(endpoint >= first and (endpoint - first) % stride == 0 and (endpoint - first) // stride < count,
            "endpoint does not belong to group")
    return first + stride * (((endpoint - first) // stride - 1) % count)


def persistent_listeners(spec, control_fd=0):
    """Own fixed listeners until STOP; route connected sockets, never listeners."""
    target = parse_search(spec["search"])
    fixed = validate_fixed_ports(spec["fixed_ports"])
    run_id, lifetime = spec["run_id"], spec["hold_seconds"]
    require(str(uuid.UUID(run_id)) == run_id, "invalid run UUID")
    require(type(lifetime) is int and 1 <= lifetime <= 3600, "hold_seconds must be 1..3600")
    nranks = spec["nranks"]
    require(type(nranks) is int and 0 < nranks <= 65536 and nranks % 4 == 0,
            "invalid persistent rank mapping")
    previous_ip = ipv4(spec["previous_ip"])
    require(target.count == 4 and target.first_rank % 4 == 0 and target.first_rank + 4 <= nranks,
            "persistent helper rank mapping differs")
    strided = spec.get("strided_groups", False)
    require(type(strided) is bool, "invalid strided group mode")
    node_ips = spec.get("node_ips")
    if strided:
        require(isinstance(node_ips, list) and len(node_ips) == nranks // 4,
                "strided helper requires complete endpoint pool")
        node_ips = [ipv4(ip) for ip in node_ips]
        require(len(set(node_ips)) == len(node_ips) and node_ips[target.first_rank // 4] == target.ip,
                "strided node identity/order differs")
    version = "4" if strided else "3"
    deadline = time.monotonic() + lifetime
    report = {"run_id": run_id, "ip": target.ip, "hostname": socket.gethostname(),
              "interface": target.interface, "pid": os.getpid(),
              "start_ticks": Path("/proc/self/stat").read_text().rsplit(")", 1)[1].split()[19],
              "started_at": utc_now(), "port_range": [target.port_start, target.port_end],
              "persistent": True, "strided_groups": strided, "fixed_ports": fixed, "hold_seconds": lifetime}
    selector = selectors.DefaultSelector()
    listeners, clients, incoming, registrations, waiting, retired = {}, {}, {}, {}, {}, set()
    directory = server = None
    stop_at = None
    control_buffer = bytearray()
    delivered = rejected = 0
    status = 2

    def drop(connection):
        if connection.fileno() in selector.get_map():
            selector.unregister(connection)
        entry = clients.pop(connection, None)
        if entry is not None:
            key = entry["key"]
            if key is not None:
                registrations.pop(key, None)
                retired.add(key)
                peer = waiting.pop(key, None)
                if peer is not None:
                    drop(peer)
            if entry["peer"] is not None:
                entry["peer"].close()
        else:
            entry = incoming.pop(connection)
            if entry["key"] is not None:
                waiting.pop(entry["key"], None)
        connection.close()

    def reject(connection, message):
        nonlocal rejected
        rejected += 1
        print(f"ERROR: persistent bootstrap: {message}", file=sys.stderr, flush=True)
        if connection in clients:
            try:
                connection.send(("ERR" + version + " " + message[:160]).encode())
            except OSError as error:
                print(f"ERROR: delivering broker error: {error}", file=sys.stderr, flush=True)
        drop(connection)

    def deliver(key):
        client, peer = registrations.get(key), waiting.get(key)
        if client is None or peer is None or clients[client]["phase"] != "wait":
            return
        entry = clients[client]
        if strided and incoming[peer]["source"] != entry["previous"]:
            reject(peer, "incoming source differs from registered group predecessor")
            return
        entry["peer"] = peer
        waiting.pop(key)
        incoming.pop(peer)
        if peer.fileno() in selector.get_map():
            selector.unregister(peer)
        entry.update(phase="send_fd", deadline=time.monotonic() + 5)
        selector.modify(client, selectors.EVENT_WRITE, "client")

    try:
        require(report["hostname"].casefold() == target.hostname.casefold(), "helper hostname mismatch")
        check_interface(target)
        ports = []
        for index, port in enumerate(fixed):
            listener = None
            operation = "socket"
            try:
                listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                operation = "bind"
                listener.bind((target.ip, port))
                operation = "listen"
                listener.listen(16384)
                operation = "setblocking"
                listener.setblocking(False)
            except OSError as error:
                if listener is not None:
                    listener.close()
                report["failed_endpoint"] = {
                    "ip": target.ip, "port": port, "operation": operation,
                    "errno": error.errno, "error_name": errno.errorcode.get(error.errno),
                }
                raise OSError(error.errno,
                              f"{target.hostname} {target.ip}:{port}: {operation} failed: "
                              f"{error.strerror or str(error)}") from error
            rank = target.first_rank + index
            listeners[listener] = rank
            selector.register(listener, selectors.EVENT_READ, "listener")
            ports.append({"rank": rank, "port": port, "status": "available",
                          "socket_inode": os.fstat(listener.fileno()).st_ino})
        directory = tempfile.mkdtemp(prefix=f"nccl-bootstrap.{run_id}.")
        os.chmod(directory, 0o700)
        endpoint = str(Path(directory) / "handoff.sock")
        server = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        server.bind(endpoint)
        os.chmod(endpoint, 0o600)
        server.listen(256)
        server.setblocking(False)
        selector.register(server, selectors.EVENT_READ, "server")
        selector.register(control_fd, selectors.EVENT_READ, "control")
        print(json.dumps({**report, "outcome": "prepared", "socket_path": endpoint,
                          "ports": ports, "skipped_ports": [], "sockets_closed": False,
                          "ready_at": utc_now()}), flush=True)
        while True:
            now = time.monotonic()
            require(now < deadline, "persistent listener lifetime expired before explicit shutdown")
            if stop_at is not None:
                if not clients and not incoming:
                    status = 0
                    break
                require(now < stop_at, "shutdown requested with live registrations or pending connections")
            for connection, entry in list(clients.items()) + list(incoming.items()):
                if now >= entry["deadline"] and (connection in clients or connection in incoming):
                    reject(connection, "connection/registration handshake timed out")
            for selected, mask in selector.select(min(0.2, deadline - now)):
                connection, kind = selected.fileobj, selected.data
                if kind == "control":
                    chunk = os.read(control_fd, 64)
                    require(bool(chunk), "controller disconnected without STOP")
                    control_buffer.extend(chunk)
                    require(len(control_buffer) <= 5, "invalid controller command")
                    if b"\n" in control_buffer:
                        require(control_buffer == b"STOP\n" and stop_at is None, "invalid controller command")
                        stop_at = time.monotonic() + 5
                    continue
                if kind in ("listener", "server"):
                    peer, _ = connection.accept()
                    peer.setblocking(False)
                    if stop_at is not None or len(clients) + len(incoming) >= 512:
                        peer.close()
                        print("ERROR: persistent broker stopping or connection capacity exhausted", file=sys.stderr, flush=True)
                        rejected += 1
                        continue
                    if kind == "server":
                        _, uid, _ = struct.unpack("3i", peer.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
                        if uid != os.geteuid():
                            peer.close()
                            print("ERROR: persistent broker client UID mismatch", file=sys.stderr, flush=True)
                            rejected += 1
                            continue
                        clients[peer] = {"key": None, "peer": None, "phase": "register",
                                         "deadline": time.monotonic() + 5}
                        selector.register(peer, selectors.EVENT_READ, "client")
                    else:
                        incoming[peer] = {"key": None, "rank": listeners[connection], "buffer": bytearray(),
                                          "deadline": time.monotonic() + 30}
                        selector.register(peer, selectors.EVENT_READ, "incoming")
                    continue
                if connection not in clients and connection not in incoming:
                    continue
                try:
                    if kind == "incoming":
                        entry = incoming[connection]
                        if entry["key"] is not None:
                            # Once framed, wait without consuming MRC metadata.
                            continue
                        chunk = connection.recv(40 - len(entry["buffer"]))
                        require(bool(chunk), "peer closed before routing header")
                        entry["buffer"].extend(chunk)
                        if len(entry["buffer"]) != 40:
                            continue
                        magic, socket_type = struct.unpack("=Qi", entry["buffer"][:12])
                        marker, job, source, destination = struct.unpack("!4s16sII", entry["buffer"][12:])
                        require(magic != 0 and socket_type == 4 and marker == ("MRC" + version).encode() and
                                job == uuid.UUID(run_id).bytes and destination == entry["rank"] and
                                source < nranks, "invalid routing identity")
                        require(connection.getpeername()[0] == (node_ips[source // 4] if strided else
                                previous_ip if destination == target.first_rank else target.ip) and
                                (strided or source == (destination - 1) % nranks), "invalid routing peer")
                        entry["source"] = source
                        key = (magic, destination)
                        require(key not in retired and key not in waiting and
                                (key not in registrations or clients[registrations[key]]["phase"] == "wait"),
                                "duplicate or retired incoming connection")
                        entry["key"] = key
                        waiting[key] = connection
                        selector.unregister(connection)
                        # Retain ownership and deadline without polling unread metadata.
                        deliver(key)
                    else:
                        entry = clients[connection]
                        phase = entry["phase"]
                        if mask & selectors.EVENT_WRITE:
                            if phase == "send_fd":
                                require(connection.sendmsg([("FD" + version).encode()], [(socket.SOL_SOCKET, socket.SCM_RIGHTS,
                                        array.array("i", [entry["peer"].fileno()]))]) == 3, "short connected-FD handoff")
                                entry["phase"] = "ack"
                            else:
                                payload = (("OK" if phase == "ok" else "DONE") + version).encode()
                                require(connection.send(payload) == len(payload), "short broker response")
                                entry["phase"] = "wait" if phase == "ok" else "active"
                                entry["deadline"] = time.monotonic() + 30 if phase == "ok" else deadline
                                if phase == "done":
                                    delivered += 1
                            selector.modify(connection, selectors.EVENT_READ, "client")
                            if entry["phase"] == "wait":
                                deliver(entry["key"])
                            continue
                        payload, ancillary, flags, _ = connection.recvmsg(256, socket.CMSG_SPACE(64), socket.MSG_CMSG_CLOEXEC)
                        for level, kind_, data in ancillary:
                            if level == socket.SOL_SOCKET and kind_ == socket.SCM_RIGHTS:
                                fds = array.array("i")
                                fds.frombytes(data[:len(data) - len(data) % fds.itemsize])
                                for fd in fds:
                                    os.close(fd)
                        require(not ancillary and not (flags & (socket.MSG_TRUNC | socket.MSG_CTRUNC)),
                                "unexpected client ancillary data or truncated request")
                        if not payload:
                            drop(connection)
                            continue
                        if phase == "register":
                            fields = payload.decode("ascii").split(" ")
                            require(len(fields) == (9 if strided else 6) and fields[:2] == ["REG" + version, run_id],
                                    "invalid registration")
                            token, rank, ip, port = fields[2:6]
                            require(re.fullmatch(r"[0-9a-f]{16}", token) is not None and int(token, 16) != 0,
                                    "invalid communicator token")
                            require(rank.isdecimal() and port.isdecimal(), "invalid rank/port")
                            rank, port = int(rank), int(port)
                            require(target.first_rank <= rank < target.first_rank + 4 and ip == target.ip and
                                    port == fixed[rank - target.first_rank], "registration endpoint differs")
                            if strided:
                                require(all(v.isdecimal() and len(v) <= 5 for v in fields[6:]), "invalid group integers")
                                entry["previous"] = group_predecessor(*map(int, fields[6:]), rank, nranks)
                            key = (int(token, 16), rank)
                            require(key not in registrations and key not in retired, "duplicate communicator registration")
                            require(len(registrations) < 256 and len(retired) + len(registrations) < 65536,
                                    "communicator registration capacity exhausted")
                            entry.update(key=key, phase="ok")
                            registrations[key] = connection
                            selector.modify(connection, selectors.EVENT_WRITE, "client")
                        elif phase == "ack":
                            require(payload == ("ACK" + version).encode(), "invalid connected-FD acknowledgement")
                            entry["peer"].close()
                            entry.update(peer=None, phase="done")
                            selector.modify(connection, selectors.EVENT_WRITE, "client")
                        else:
                            raise ValueError("unexpected registration message")
                except BlockingIOError:
                    continue
                except (OSError, ValueError, UnicodeError) as error:
                    reject(connection, str(error))
    except (OSError, ValueError, subprocess.TimeoutExpired, KeyboardInterrupt) as error:
        report["error"] = str(error) or "interrupted"
        print(f"ERROR: persistent bootstrap: {report['error']}", file=sys.stderr, flush=True)
    finally:
        # Incoming sockets can be unregistered while waiting for their local rank.
        for entry in clients.values():
            if entry["peer"] is not None:
                entry["peer"].close()
        for connection in (*clients, *incoming, *listeners):
            connection.close()
        if server is not None:
            server.close()
        selector.close()
        if directory is not None:
            path = Path(directory) / "handoff.sock"
            if path.exists():
                path.unlink()
            Path(directory).rmdir()
        print(json.dumps({**report, "outcome": "stopped" if status == 0 else "error",
                          "delivered_connections": delivered, "rejected_requests": rejected,
                          "owned_descriptors_closed": True, "helper_directory_removed": True,
                          "finished_at": utc_now()}), flush=True)
    return status


def hold_listeners(spec, control_fd=0):
    """Hold real listeners until same-UID ranks acknowledge descriptor ownership."""
    target = parse_search(spec["search"])
    require(target.count == 4 and target.first_rank % 4 == 0, "prepared mode requires four ranks/node")
    run_id, lifetime = spec["run_id"], spec["hold_seconds"]
    require(str(uuid.UUID(run_id)) == run_id, "invalid run UUID")
    require(type(lifetime) is int and 1 <= lifetime <= 3600, "hold_seconds must be 1..3600")
    deadline = time.monotonic() + lifetime
    claimed, listeners, ports, skipped = [], {}, [], []
    directory = None
    report = {"run_id": run_id, "ip": target.ip, "hostname": socket.gethostname(), "interface": target.interface,
              "pid": os.getpid(), "start_ticks": Path("/proc/self/stat").read_text().rsplit(")", 1)[1].split()[19],
              "started_at": utc_now(), "port_range": [target.port_start, target.port_end]}
    status = 2
    try:
        with ExitStack() as owned:
            require(report["hostname"].casefold() == target.hostname.casefold(), "helper hostname mismatch")
            check_interface(target)
            for port in range(target.port_start, target.port_end + 1):
                listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    listener.bind((target.ip, port))
                    listener.listen(16384)
                    listener.setblocking(False)
                except OSError as error:
                    listener.close()
                    if error.errno != errno.EADDRINUSE:
                        raise
                    skipped.append({"port": port, "status": "unavailable", "errno": error.errno, "error": str(error)})
                    continue
                rank = target.first_rank + len(ports)
                listeners[rank] = owned.enter_context(listener)
                ports.append({"rank": rank, "port": port, "status": "available",
                              "socket_inode": os.fstat(listener.fileno()).st_ino})
                if len(ports) == target.count:
                    break
            require(len(ports) == target.count, "port range exhausted")
            directory = Path(tempfile.mkdtemp(prefix=f"nccl-bootstrap.{run_id}.", dir="/tmp"))
            endpoint = str(directory / "handoff.sock")
            validate_handoff_path(endpoint, run_id)
            server = owned.enter_context(socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET))
            server.bind(endpoint)
            os.chmod(endpoint, 0o600)
            server.listen(4)
            ready = {**report, "outcome": "prepared", "socket_path": endpoint,
                     "ports": ports, "skipped_ports": skipped, "sockets_closed": False,
                     "ready_at": utc_now(), "hold_seconds": lifetime}
            print(json.dumps(ready), flush=True)
            while len(claimed) < target.count:
                remaining = deadline - time.monotonic()
                require(remaining > 0, "listener lifetime expired before all handoffs")
                readable, _, _ = select.select([server, control_fd], [], [], min(remaining, 1))
                if control_fd in readable:
                    require(os.read(control_fd, 1) != b"", "controller disconnected before all handoffs")
                    raise ValueError("unexpected controller input")
                if server not in readable:
                    continue
                connection, _ = server.accept()
                with connection:
                    connection.settimeout(min(2, max(0.001, deadline - time.monotonic())))
                    _, uid, _ = struct.unpack("3i", connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
                    require(uid == os.geteuid(), "handoff client UID mismatch")
                    request, ancillary, flags, _ = connection.recvmsg(256, socket.CMSG_SPACE(4 * 16))
                    unexpected = []
                    for level, kind, content in ancillary:
                        if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
                            fds = array.array("i")
                            fds.frombytes(content[:len(content) - len(content) % fds.itemsize])
                            unexpected.extend(fds)
                    for fd in unexpected:
                        os.close(fd)
                    require(not flags and not ancillary, "unexpected request ancillary data or truncation")
                    fields = request.decode("ascii").split(" ")
                    require(len(fields) == 5 and fields[0] == "TAKE1" and fields[1] == run_id,
                            "invalid or wrong-run handoff request")
                    rank = int(fields[2])
                    require(rank in listeners and rank not in claimed, "rank is invalid or already handed off")
                    expected = ports[rank - target.first_rank]
                    require(fields[3] == target.ip and fields[4] == str(expected["port"]), "endpoint mismatch")
                    require(time.monotonic() < deadline, "listener lifetime expired during handoff")
                    sent = connection.sendmsg([b"FD1"], [(socket.SOL_SOCKET, socket.SCM_RIGHTS,
                                                        array.array("i", [listeners[rank].fileno()]))])
                    require(sent == 3, "incomplete descriptor response")
                    connection.settimeout(min(2, max(0.001, deadline - time.monotonic())))
                    require(connection.recv(16) == b"ACK1", "descriptor ownership not acknowledged")
                    require(time.monotonic() < deadline, "listener lifetime expired before acknowledgement")
                    listeners[rank].close()
                    claimed.append(rank)
                    require(connection.send(b"DONE1") == 5, "handoff completion response failed")
            status = 0
    except (OSError, ValueError, UnicodeError, subprocess.TimeoutExpired, KeyboardInterrupt) as error:
        report["error"] = str(error) or "helper interrupted"
        print(f"ERROR: prepared listener helper: {report['error']}", file=sys.stderr, flush=True)
    finally:
        if directory is not None:
            endpoint = directory / "handoff.sock"
            if endpoint.exists():
                endpoint.unlink()
            directory.rmdir()
        report.update(outcome="handed_off" if status == 0 else "error", claimed_ranks=claimed,
                      closed_unclaimed_ranks=sorted(set(listeners) - set(claimed)),
                      owned_descriptors_closed=True, helper_directory_removed=True, finished_at=utc_now())
        print(json.dumps(report), flush=True)
    return status


def validate_held_ready(data, target, run_id, fixed_ports=None):
    require(data.get("outcome") == "prepared" and data.get("run_id") == run_id,
            f"{target.hostname} {target.ip}: helper not prepared for this run: "
            f"{data.get('error', 'no helper error detail reported')}")
    require(data.get("ip") == target.ip and isinstance(data.get("hostname"), str) and
            data["hostname"].casefold() == target.hostname.casefold() and
            data.get("interface") == target.interface and data.get("sockets_closed") is False,
            "prepared helper identity/state mismatch")
    validate_handoff_path(data.get("socket_path"), run_id)
    require(type(data.get("pid")) is int and data["pid"] > 0 and
            isinstance(data.get("start_ticks"), str) and data["start_ticks"].isdigit(), "missing helper identity")
    require(type(data.get("hold_seconds")) is int and 1 <= data["hold_seconds"] <= 3600, "invalid helper lifetime")
    if fixed_ports is not None:
        fixed = validate_fixed_ports(fixed_ports)
        require(data.get("persistent") is True and data.get("fixed_ports") == fixed and
                data.get("skipped_ports") == [], "persistent preparation mode differs")
        ports = data.get("ports")
        require(isinstance(ports, list) and len(ports) == 4, "incomplete fixed-port readiness")
        for index, row in enumerate(ports):
            require(row.get("rank") == target.first_rank + index and row.get("port") == fixed[index] and
                    row.get("status") == "available" and type(row.get("socket_inode")) is int and
                    row["socket_inode"] > 0, "fixed listener readiness differs")
        return
    # Validate the same ascending scan/coverage without pretending the live SSH has exited.
    selection = {**data, "outcome": "available", "sockets_closed": True, "finished_at": data.get("ready_at")}
    validate_selection_response(selection, target, 0)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def check_interface(node: Node | PortSearch) -> None:
    result = subprocess.run(
        ["ip", "-j", "-4", "address", "show", "dev", node.interface],
        capture_output=True, text=True, timeout=3,
    )
    require(result.returncode == 0,
            f"interface query failed (exit {result.returncode}): {result.stderr.strip()}")
    interfaces = read_json(result.stdout)
    require(isinstance(interfaces, list), "invalid interface query response")
    addresses = set()
    for interface in interfaces:
        require(isinstance(interface, dict) and isinstance(interface.get("addr_info"), list),
                "invalid interface address record")
        if interface.get("ifname") != node.interface:
            continue
        for address in interface["addr_info"]:
            require(isinstance(address, dict), "invalid interface address")
            if address.get("family") == "inet":
                addresses.add(ipv4(address.get("local")))
    require(node.ip in addresses, f"{node.ip} is not assigned to interface {node.interface}")


def bind_listener(ip: str, port: int, held_sockets: ExitStack) -> dict:
    operation = "socket"
    with ExitStack() as candidate:
        try:
            listener = candidate.enter_context(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
            operation = "bind"
            listener.bind((ip, port))
            operation = "listen"
            listener.listen(1)
        except OSError as error:
            if operation == "socket" or error.errno not in (
                errno.EADDRINUSE, errno.EADDRNOTAVAIL, errno.EACCES, errno.EPERM
            ):
                raise
            return {
                "port": port, "status": "unavailable", "operation": operation,
                "errno": error.errno, "error_name": errno.errorcode[error.errno], "error": str(error),
            }
        # Keep successful listeners together; close failed candidates immediately.
        held_sockets.enter_context(candidate.pop_all())
    return {"port": port, "status": "available"}


def probe_local(node: Node) -> dict:
    result = {
        "ip": node.ip, "hostname": socket.gethostname(), "interface": node.interface,
        "started_at": utc_now(), "outcome": "error", "sockets_closed": False,
        "ports": [{"rank": entry.rank, "port": entry.port, "status": "not_checked"}
                  for entry in node.ports],
    }
    try:
        require(result["hostname"].casefold() == node.hostname.casefold(),
                f"hostname mismatch: expected {node.hostname}, observed {result['hostname']}")
        check_interface(node)
    except (OSError, ValueError, subprocess.TimeoutExpired) as error:
        result.update(error=str(error), finished_at=utc_now(), sockets_closed=True)
        return result
    with ExitStack() as sockets:
        for entry in result["ports"]:
            entry.update(bind_listener(node.ip, entry["port"], sockets))
    result.update(
        outcome="available" if all(entry["status"] == "available" for entry in result["ports"])
        else "unavailable",
        sockets_closed=True, finished_at=utc_now(),
    )
    return result


def select_ports_local(search: PortSearch) -> dict:
    result = {
        "ip": search.ip, "hostname": socket.gethostname(), "interface": search.interface,
        "started_at": utc_now(), "outcome": "error", "sockets_closed": False,
        "port_range": [search.port_start, search.port_end], "ports": [], "skipped_ports": [],
    }
    try:
        require(result["hostname"].casefold() == search.hostname.casefold(),
                f"hostname mismatch: expected {search.hostname}, observed {result['hostname']}")
        check_interface(search)
    except (OSError, ValueError, subprocess.TimeoutExpired) as error:
        result.update(error=str(error), finished_at=utc_now(), sockets_closed=True)
        return result
    with ExitStack() as sockets:
        for port in range(search.port_start, search.port_end + 1):
            attempt = bind_listener(search.ip, port, sockets)
            if attempt["status"] == "available":
                result["ports"].append({"rank": search.first_rank + len(result["ports"]), **attempt})
                if len(result["ports"]) == search.count:
                    break
            elif attempt["errno"] == errno.EADDRINUSE:
                result["skipped_ports"].append(attempt)
            else:
                result.update(error=f"port {port}: {attempt['error']}", failed_port=attempt)
                break
    if "error" not in result:
        result["outcome"] = "available" if len(result["ports"]) == search.count else "unavailable"
        if result["outcome"] == "unavailable":
            result["reason"] = f"range exhausted: found {len(result['ports'])} of {search.count} ports"
    result.update(sockets_closed=True, finished_at=utc_now())
    return result


def validate_probe_header(data, node: Node | PortSearch, exit_code: int) -> str:
    require(isinstance(data, dict), "helper response is not an object")
    require(data.get("ip") == node.ip and data.get("interface") == node.interface,
            "helper reported a different endpoint/interface")
    require(data.get("sockets_closed") is True, "helper did not confirm socket cleanup")
    outcome = data.get("outcome")
    require(outcome in ("available", "unavailable", "error"), "invalid helper outcome")
    require(exit_code == {"available": 0, "unavailable": 1, "error": 2}[outcome],
            "helper exit code disagrees with response")
    for field in ("started_at", "finished_at"):
        require(isinstance(data.get(field), str), f"missing helper {field}")
        require(datetime.fromisoformat(data[field]).utcoffset() is not None,
                "helper timestamps must include timezone")
    require(datetime.fromisoformat(data["finished_at"]) >= datetime.fromisoformat(data["started_at"]),
            "helper timestamps are reversed")
    if outcome == "error":
        require(isinstance(data.get("error"), str) and bool(data["error"]),
                "missing helper error detail")
    else:
        require(isinstance(data.get("hostname"), str) and
                data["hostname"].casefold() == node.hostname.casefold(), "helper hostname mismatch")
    return outcome


def validate_response(data, node: Node, exit_code: int) -> None:
    outcome = validate_probe_header(data, node, exit_code)
    ports = data.get("ports")
    require(isinstance(ports, list) and len(ports) == len(node.ports), "incomplete port results")
    for entry, expected in zip(ports, node.ports):
        require(isinstance(entry, dict) and type(entry.get("rank")) is int and
                type(entry.get("port")) is int and entry["rank"] == expected.rank and
                entry["port"] == expected.port, "helper port/rank coverage mismatch")
        require(entry.get("status") in ("available", "unavailable", "not_checked"),
                "invalid port status")
        if entry["status"] == "unavailable":
            require(isinstance(entry.get("error"), str) and bool(entry["error"]),
                    "missing port failure detail")
    if outcome == "error":
        require(all(entry["status"] == "not_checked" for entry in ports),
                "setup error must not report probed ports")
    else:
        require(all(entry["status"] != "not_checked" for entry in ports), "unprobed ports")
        all_available = all(entry["status"] == "available" for entry in ports)
        require((outcome == "available") == all_available, "helper outcome contradicts port results")


def validate_selection_response(data, search: PortSearch, exit_code: int) -> None:
    outcome = validate_probe_header(data, search, exit_code)
    require(data.get("port_range") == [search.port_start, search.port_end], "wrong search range")
    ports, skipped = data.get("ports"), data.get("skipped_ports")
    require(isinstance(ports, list) and isinstance(skipped, list), "missing selection results")
    require(len(ports) <= search.count, "too many selected ports")
    for index, entry in enumerate(ports):
        require(isinstance(entry, dict) and type(entry.get("rank")) is int and
                entry["rank"] == search.first_rank + index and entry.get("status") == "available",
                "selected rank/status mismatch")
    for entry in skipped:
        require(isinstance(entry, dict) and entry.get("status") == "unavailable" and
                type(entry.get("errno")) is int and entry["errno"] == errno.EADDRINUSE and
                isinstance(entry.get("error"), str) and bool(entry["error"]),
                "only occupied ports may be skipped")
    numbers = []
    for entry in ports + skipped:
        port = entry.get("port")
        require(type(port) is int and search.port_start <= port <= search.port_end,
                "selected/skipped port outside search range")
        numbers.append(port)
    require(len(numbers) == len(set(numbers)), "duplicate selected/skipped port")
    require(sorted(numbers) == list(range(search.port_start, search.port_start + len(numbers))),
            "selection has gaps in ascending port scan")
    require([entry["port"] for entry in ports] == sorted(entry["port"] for entry in ports),
            "selected ports are not ascending")
    if outcome == "available":
        require(len(ports) == search.count and numbers and max(numbers) == ports[-1]["port"],
                "incomplete or inconsistent successful selection")
    elif outcome == "unavailable":
        require(len(ports) < search.count and len(numbers) == search.port_end - search.port_start + 1,
                "range exhaustion not established")


def output_text(value: str | bytes | None) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value if value is not None else ""


def probe_remote(node: Node | PortSearch, source: str) -> dict:
    selecting = isinstance(node, PortSearch)
    payload = json.dumps(asdict(node), separators=(",", ":"))
    remote = shlex.join([
        "timeout", "--signal=TERM", "--kill-after=2s", f"{PROBE_SECONDS}s",
        "python3", "-", "--select-worker" if selecting else "--worker", payload,
    ])
    command = ["ssh", *SSH_OPTIONS, f"azhpcuser@{node.ip}", remote]
    failure = {
        "ip": node.ip, "expected_hostname": node.hostname, "outcome": "error",
        "sockets_closed": None, "ssh_exit_code": None, "command": command,
        "ports": [{"rank": entry.rank, "port": entry.port, "status": "not_checked"}
                  for entry in node.ports] if not selecting else [],
    }
    if selecting:
        failure.update(port_range=[node.port_start, node.port_end], skipped_ports=[])
    try:
        result = subprocess.run(command, input=source, capture_output=True, text=True, timeout=SSH_SECONDS)
    except subprocess.TimeoutExpired as error:
        failure.update(error=f"SSH exceeded {SSH_SECONDS}s; remote result/cleanup unconfirmed",
                       stdout=output_text(error.stdout), stderr=output_text(error.stderr))
        return failure
    except (OSError, UnicodeError) as error:
        failure["error"] = f"could not complete SSH: {error}"
        return failure
    try:
        require(result.returncode in (0, 1, 2), f"SSH/helper exited {result.returncode}")
        report = read_json(result.stdout)
        if selecting:
            validate_selection_response(report, node, result.returncode)
        else:
            validate_response(report, node, result.returncode)
    except ValueError as error:
        failure.update(error=f"invalid or missing helper result: {error}",
                       ssh_exit_code=result.returncode, stdout=result.stdout, stderr=result.stderr)
        return failure
    report.update(ssh_exit_code=result.returncode, ssh_stderr=result.stderr, command=command)
    return report


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv and argv[0] in ("--hold-worker", "--persistent-worker"):
        def interrupted(signum, frame):
            raise KeyboardInterrupt
        signal.signal(signal.SIGTERM, interrupted)
        try:
            require(len(argv) == 2, "hold worker requires one specification")
            worker = persistent_listeners if argv[0] == "--persistent-worker" else hold_listeners
            return worker(read_json(argv[1]))
        except (OSError, ValueError, KeyError) as error:
            print(f"ERROR: {error}", file=sys.stderr)
            return 2
    if argv and argv[0] in ("--worker", "--select-worker"):
        try:
            require(len(argv) == 2, "internal worker requires one JSON node specification")
            if argv[0] == "--select-worker":
                result = select_ports_local(parse_search(read_json(argv[1])))
            else:
                result = probe_local(parse_node(read_json(argv[1])))
        except (OSError, ValueError) as error:
            print(f"ERROR: {error}", file=sys.stderr)
            return 2
        print(json.dumps(result))
        return {"available": 0, "unavailable": 1, "error": 2}[result["outcome"]]
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("manifest", nargs="?", type=Path, default=Path("static-bootstrap.json"))
    parser.add_argument("--probe", action="store_true",
                        help="explicitly authorize temporary remote port probes; default only validates the plan")
    args = parser.parse_args(argv)
    try:
        raw = args.manifest.read_bytes()
        data = read_json(raw)
        run_id, nodes = parse_manifest(data)
        require(not args.probe or data["schema_version"] == 1,
                "prepared listeners are already bound; use the live preparation controller's status, not --probe")
        source = Path(__file__).read_text() if args.probe else None
    except (OSError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 2
    report = {
        "run_id": run_id, "manifest": str(args.manifest.resolve()),
        "manifest_sha256": hashlib.sha256(raw).hexdigest(),
        "observed_at": utc_now(), "outcome": "not_probed", "ports_reserved": False,
        "caveat": "Point-in-time local bind/listen only; not a reservation, reachability or NCCL readiness check.",
        "nodes": [],
    }
    if not args.probe:
        report["nodes"] = [{**asdict(node), "outcome": "not_probed"} for node in nodes]
        print(json.dumps(report, indent=2))
        return 0
    for node in nodes:
        result = probe_remote(node, source)
        report["nodes"].append(result)
        if result["outcome"] == "error":
            for skipped in nodes[len(report["nodes"]):]:
                report["nodes"].append({
                    **asdict(skipped), "outcome": "not_probed",
                    "reason": "stopped after an infrastructure/protocol error",
                })
            break
    outcomes = {node["outcome"] for node in report["nodes"]}
    report["outcome"] = ("error" if "error" in outcomes else
                         "unavailable" if "unavailable" in outcomes else "available")
    print(json.dumps(report, indent=2))
    return {"available": 0, "unavailable": 1, "error": 2}[report["outcome"]]


if __name__ == "__main__":
    sys.exit(main())
