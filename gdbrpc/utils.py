############################################################################
# gdbrpc/utils.py
#
# SPDX-License-Identifier: Apache-2.0
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.  The
# ASF licenses this file to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance with the
# License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
# License for the specific language governing permissions and limitations
# under the License.
#
############################################################################

import json
import logging
import os
import queue
import socket
import struct
import subprocess
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import Enum, IntEnum
from typing import Any, Optional, Tuple

FRAME_HEADER_SIZE = 4


def recv_all(connection: socket.socket, length: int) -> bytes:
    data = b""
    while len(data) < length:
        chunk = connection.recv(length - len(data))
        if not chunk:
            raise ConnectionError("Socket connection broken during receive")
        data += chunk
    return data


def socket_recv(connection: socket.socket) -> bytes:
    length_data = recv_all(connection, FRAME_HEADER_SIZE)
    # https://docs.python.org/3.10/library/struct.html#format-characters
    data_length = struct.unpack("!I", length_data)[0]
    return recv_all(connection, data_length)


def send_all(connection: socket.socket, data: bytes) -> None:
    total_sent = 0
    while total_sent < len(data):
        sent = connection.send(data[total_sent:])
        if sent == 0:
            raise RuntimeError("Socket connection broken")
        total_sent += sent


def socket_send(connection: socket.socket, data: bytes) -> None:
    length_prefix = struct.pack("!I", len(data))
    send_all(connection, length_prefix)
    send_all(connection, data)


def make_session_uuid() -> str:
    return f"sess-{uuid.uuid4().hex[:8]}"


def format_endpoint(endpoint: Optional[Tuple[str, int]]) -> str:
    if not endpoint:
        return "unknown"
    host, port = endpoint
    return f"{host}:{port}"


def truncate_text(text: str, limit: Optional[int] = 256) -> str:
    if limit is None or len(text) <= limit:
        return text
    return f"{text[:limit]}...[truncated,total={len(text)}]"


def setup_file_logging(
    name: str,
    prefix: str,
    log_level: int,
    log_path: Optional[str] = None,
    human_log_path: Optional[str] = None,
) -> Tuple[logging.Logger, logging.Logger]:
    """Configure structured and human-readable file loggers for one role.

    The default file names embed a timestamp and pid that are computed once
    and shared by both loggers. Returns (logger, human_logger).
    """
    formatter = logging.Formatter(f"%(asctime)s {prefix}: %(message)s")
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    pid = os.getpid()

    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    if not logger.handlers:
        logger.propagate = False
        if log_path is None:
            log_path = f"{prefix}-{timestamp}-pid{pid}.log"
        handler = logging.FileHandler(log_path)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    human_logger = logging.getLogger(f"{name}.human")
    human_logger.setLevel(log_level)
    if not human_logger.handlers:
        human_logger.propagate = False
        if human_log_path is None:
            human_log_path = f"{prefix}-human-{timestamp}-pid{pid}.log"
        human_handler = logging.FileHandler(human_log_path)
        human_handler.setFormatter(formatter)
        human_logger.addHandler(human_handler)

    return logger, human_logger


def format_human_event(
    role: str,
    event: "EventType",
    request: Optional["Request"] = None,
    **fields: Any,
) -> Optional[str]:
    """Format important RPC lifecycle events for a human reader."""
    title = HUMAN_EVENT_NAMES.get((role, event))
    if title is None:
        return None

    details = []
    if request is not None:
        details.append(f"request={request.req_seq or 'unknown'}")
        details.append(f"type={request.__class__.__name__}")
    for key in ("status", "target", "source", "callback", "error", "note"):
        value = fields.get(key)
        if value is not None:
            details.append(f"{key}={value}")

    lines = [title + (f" ({', '.join(details)})" if details else "")]
    for key, label in (
        ("dump", "request"),
        ("callback_dump", "callback"),
        ("payload_full", "response"),
        ("payload", "payload"),
    ):
        value = fields.get(key)
        if value is None:
            continue
        try:
            parsed = json.loads(value) if isinstance(value, str) else value
            text = json.dumps(parsed, indent=2, ensure_ascii=False)
        except (TypeError, ValueError):
            text = str(value)
        lines.append(f"  {label}:")
        lines.extend(f"    {line}" for line in text.splitlines() or [""])

    return "\n".join(lines) + "\n"


# Keys carried only for the human-readable log; skipped by the structured
# one-line log to keep it compact (e.g. full untruncated payloads).
HUMAN_ONLY_FIELDS = frozenset({"payload_full"})


class EventType(str, Enum):
    CONNECT = "connect"
    DISCONNECT = "disconnect"
    SEND = "send"
    RECV = "recv"
    CALLBACK = "callback"
    ERROR = "error"
    TIMEOUT = "timeout"
    DISPATCH = "dispatch"
    DONE = "done"
    REPLY = "reply"
    TRANSPORT_SEND_LEN = "transport_send_len"
    TRANSPORT_SEND_BODY = "transport_send_body"
    TRANSPORT_RECV_LEN = "transport_recv_len"
    TRANSPORT_RECV_BODY = "transport_recv_body"


HUMAN_EVENT_NAMES = {
    ("client", EventType.SEND): "SEND REQUEST",
    ("server", EventType.RECV): "RECEIVE REQUEST",
    ("server", EventType.DISPATCH): "DISPATCH GDB COMMAND",
    ("server", EventType.REPLY): "SEND ACK",
    ("server", EventType.DONE): "RECEIVE GDB RESPONSE",
    ("client", EventType.RECV): "RECEIVE RESPONSE",
    ("client", EventType.CALLBACK): "CALLBACK RESULT",
    ("client", EventType.CONNECT): "CONNECT",
    ("client", EventType.DISCONNECT): "DISCONNECT",
    ("server", EventType.CONNECT): "CONNECT",
    ("server", EventType.DISCONNECT): "DISCONNECT",
    ("client", EventType.ERROR): "ERROR",
    ("server", EventType.ERROR): "ERROR",
    ("client", EventType.TIMEOUT): "TIMEOUT",
    ("server", EventType.TIMEOUT): "TIMEOUT",
}


@dataclass
class RequestContext:
    request: "Request"
    status: "PacketStatus"
    callback_type: Optional[str] = None
    callback_dump: Optional[str] = None


class PacketStatus(IntEnum):
    HAS_CALLBACK = 0
    NO_CALLBACK = 1
    PYTHON_VERSION_MISMATCH = 2


class Response:
    def __init__(self, tag: int, payload: Any):
        self.tag = tag
        self.payload = payload


class Request:
    # Internal bookkeeping fields that dump() must never include.
    _DUMP_EXCLUDE = frozenset({"tag", "req_seq", "session_uuid", "peer"})

    def __init__(self):
        self.tag = id(self)
        self.req_seq: Optional[str] = None
        self.session_uuid: Optional[str] = None
        self.peer: Optional[str] = None

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        raise NotImplementedError("Subclasses must implement this method")

    def dump(self) -> str:
        """Return a JSON string representation of this request.

        By default this dumps every instance attribute as a field, producing
        a deterministic JSON string. Internal bookkeeping fields (tag,
        req_seq, session_uuid, peer) and any value that cannot be serialized
        to JSON (e.g. threading.Event) are skipped. Subclasses may override
        this method to customize the output, or extend _DUMP_EXCLUDE to hide
        additional internal fields.
        """
        fields = {}
        for name, value in self.__dict__.items():
            if name in self._DUMP_EXCLUDE:
                continue
            try:
                json.dumps(value)
            except (TypeError, ValueError):
                continue
            fields[name] = value
        return json.dumps(
            {"type": self.__class__.__name__, "fields": fields},
            sort_keys=True,
            separators=(",", ":"),
        )


class PostRequest(Request):
    def __init__(self):
        super().__init__()
        self.finish = threading.Event()

    def __call__(self, argument: Any):
        raise NotImplementedError("Subclasses must implement this method")

    def callback_dump(self) -> str:
        """Return a JSON string representation of this callback.

        Subclasses MUST override this method if they have additional fields
        to dump beyond what dump() provides.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__}.callback_dump() must be implemented by subclass"
        )


class ShellExec(Request):
    def __init__(self, command: str):
        super().__init__()

        self.is_gdb_command = True
        command = command.strip()

        if command.startswith("!"):
            command = command[1:].strip()
            self.is_gdb_command = False
        elif command.startswith("shell"):
            command = command[5:].strip()
            self.is_gdb_command = False

        self.command = command

    def _run_shell_command(self, command) -> str:
        try:
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                universal_newlines=True,
            )

            result = ""

            while True:

                stdout_line = process.stdout.readline()
                stderr_line = process.stderr.readline()

                if stdout_line:
                    result += f"{stdout_line}"
                if stderr_line:
                    result += f"{stderr_line}"

                if (
                    stdout_line == ""
                    and stderr_line == ""
                    and process.poll() is not None
                ):
                    break

            return result
        except Exception as e:
            return f"Error executing command '{command}': {e}"

    def __call__(self, queue: queue.Queue):
        import gdb

        try:
            if self.is_gdb_command:
                out = gdb.execute(f"{self.command}", to_string=True)
            else:
                out = self._run_shell_command(self.command.split())
        except Exception as e:
            out = e
        queue.put(out)
