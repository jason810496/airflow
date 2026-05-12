#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Fake foreign-runtime subprocess for supervisor IPC integration tests.

Plays the role a coordinator's runtime subprocess plays in production
(Java, Go, Rust, ...) but with the smallest possible footprint: it
connects to a TCP comm socket, reads length-prefixed msgpack frames,
records their bodies to a JSON file, and sends pre-queued msgpack
frames back to the supervisor. The test driver inspects the captured
JSON to assert what wire shape the runtime received from the
supervisor, and reads off the supervisor end of the socket to assert
what wire shape the runtime sent upstream.

CLI:

  --comm-addr host:port   TCP address of the supervisor comm server.
  --captured-out PATH     JSON file the runtime writes after exit. The
                          file contains a list of frame bodies (the
                          msgpack-decoded ``body`` field of each
                          ``_ResponseFrame``); the framing envelope
                          itself is dropped because the per-frame test
                          assertions are body-shape only.
  --send-frames PATH      Optional JSON file with a list of request
                          frames to send upstream. Each frame is a
                          three-element list ``[id, body, context]``
                          mirroring the on-wire ``_RequestFrame``
                          tuple. Sent verbatim, in order, after the
                          first ``--read-count`` frames have been
                          consumed.
  --read-count N          Frames to read from the supervisor before
                          sending the queued upstream frames. Defaults
                          to 1, matching the typical
                          ``StartupDetails`` / ``DagFileParseRequest``
                          seed.
  --final-read-count N    Frames to read after the upstream send phase
                          finishes. Defaults to 0; used by tests that
                          want to assert on the supervisor's reply to
                          a runtime-originated request.

Wire protocol matches :mod:`airflow.sdk.execution_time.comms`:
4-byte big-endian length prefix followed by a msgpack-packed
``[id, body, ...]`` tuple. ``msgpack`` is used (not ``msgspec``) so
the helper has zero dependency on the supervisor's framing classes.
"""

from __future__ import annotations

import argparse
import json
import socket
import sys
from pathlib import Path

import msgpack


def _recv_exact(sock: socket.socket, n: int) -> bytes | None:
    """Read exactly *n* bytes from *sock*, or return ``None`` on EOF."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)


def _read_frame(sock: socket.socket) -> list | None:
    """Read one length-prefixed msgpack frame; return the decoded tuple as a list."""
    length_bytes = _recv_exact(sock, 4)
    if length_bytes is None:
        return None
    length = int.from_bytes(length_bytes, "big")
    payload = _recv_exact(sock, length)
    if payload is None:
        return None
    decoded = msgpack.unpackb(payload)
    return list(decoded)


def _write_frame(sock: socket.socket, frame: list) -> None:
    """Write *frame* as a length-prefixed msgpack frame to *sock*."""
    payload = msgpack.packb(tuple(frame))
    sock.sendall(len(payload).to_bytes(4, "big") + payload)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--comm-addr", required=True)
    parser.add_argument("--captured-out", required=True)
    parser.add_argument("--send-frames", default=None)
    parser.add_argument("--read-count", type=int, default=1)
    parser.add_argument("--final-read-count", type=int, default=0)
    args = parser.parse_args(argv)

    host, port_str = args.comm_addr.rsplit(":", 1)
    port = int(port_str)

    sock = socket.socket()
    sock.connect((host, port))

    captured_bodies: list = []

    # Phase 1 -- consume the supervisor's downgraded frames.
    for _ in range(args.read_count):
        frame = _read_frame(sock)
        if frame is None:
            break
        # frame[0] is the request/response id, frame[1] is the body dict.
        # Body shape is what the test inspects to verify the downgrade.
        body = frame[1] if len(frame) > 1 else None
        captured_bodies.append(body)

    # Phase 2 -- replay any queued runtime-side request frames upstream.
    if args.send_frames:
        outbound = json.loads(Path(args.send_frames).read_text())
        for frame in outbound:
            _write_frame(sock, frame)

    # Phase 3 -- consume any responses the supervisor sends after our requests.
    for _ in range(args.final_read_count):
        frame = _read_frame(sock)
        if frame is None:
            break
        body = frame[1] if len(frame) > 1 else None
        captured_bodies.append(body)

    Path(args.captured_out).write_text(json.dumps(captured_bodies, default=str))
    sock.close()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
