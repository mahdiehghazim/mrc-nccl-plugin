#!/usr/bin/env python3
r"""Prepare persistent NCCL bootstrap listeners on four reserved ports per node.

    python3 prepare_nccl_bootstrap.py --allocation approved-allocation.json \
        --output static-bootstrap.json --hold-seconds 1800

This command starts remote preparation immediately, with up to 128 concurrent
helper startups by default. It binds exactly the configured ports, never
searches for alternatives, and publishes a schema-4 manifest only after all
helpers are ready. Keep the
controller attached; send STOP followed by a newline on stdin after NCCL
communicator/workload cleanup. Expiry or unexpected EOF is an error.

The manifest is not distributed to the nodes by this command. No node
reservation, NCCL workload, installation or system configuration is performed.
See tools/bootstrap/README.md for allocation requirements and timer boundaries.
Keep create_static_bootstrap.py and check_bootstrap_ports.py alongside this file;
they provide the shared, tested controller and node-local worker implementation.
"""

import argparse
from datetime import datetime, timezone
import hashlib
from pathlib import Path
import sys
import uuid

import check_bootstrap_ports as checker
import create_static_bootstrap as bootstrap


RESERVED_PORTS = (27000, 27001, 27002, 27003)
DEFAULT_STARTUP_CONCURRENCY = 128


def main(argv: list[str] | None = None, *, control_fd=None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
        allow_abbrev=False,
    )
    parser.add_argument("--allocation", type=Path, required=True,
                        help="fresh approved allocation; all nodes are used in their recorded order")
    parser.add_argument("--output", type=Path, required=True,
                        help="new manifest path; an existing path is never overwritten")
    parser.add_argument("--hold-seconds", type=int, required=True,
                        help="finite helper lifetime from startup, 1..3600 seconds; not a startup delay")
    parser.add_argument("--startup-concurrency", type=int, default=DEFAULT_STARTUP_CONCURRENCY,
                        help="maximum simultaneous helper startups; positive integer "
                             "(default: %(default)s, limited to the allocation's node count)")
    parser.add_argument("--ports", type=int, nargs=4, default=list(RESERVED_PORTS), metavar="PORT",
                        help="four exact reserved ports in local-GPU order (default: 27000 27001 27002 27003)")
    args = parser.parse_args(argv)
    try:
        checker.require(1 <= args.hold_seconds <= 3600, "--hold-seconds must be in 1..3600")
        checker.require(args.startup_concurrency > 0, "--startup-concurrency must be positive")
        ports = checker.validate_fixed_ports(args.ports)
        checker.require(args.output.parent.is_dir(), "output directory does not exist")
        checker.require(not args.output.exists() and not args.output.is_symlink(), "output path already exists")
        raw = args.allocation.read_bytes()
        allocation = checker.read_json(raw)
        checker.require(isinstance(allocation, dict), "allocation must be an object")
        nodes = allocation.get("nodes")
        checker.require(isinstance(nodes, list) and 1 <= len(nodes) <= 65536 // bootstrap.RANKS_PER_NODE,
                        "allocation must contain 1..16384 nodes")
        selected, _ = bootstrap.validate_allocation(
            allocation, len(nodes), datetime.now(timezone.utc), args.hold_seconds)
        endpoints = [bootstrap.make_search(node, allocation["socket_ifname"], index, ports)
                     for index, node in enumerate(selected)]
    except (OSError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 2

    return bootstrap.prepare_and_hold(
        allocation, selected, endpoints, str(uuid.uuid4()), hashlib.sha256(raw).hexdigest(),
        args.output, args.hold_seconds, min(len(selected), args.startup_concurrency),
        ports, control_fd, strided_groups=True,
    )


if __name__ == "__main__":
    sys.exit(main())
