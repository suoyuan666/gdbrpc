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
import queue
import socket
import struct
import subprocess
import threading
import uuid
from dataclasses import dataclass
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


def truncate_text(text: str, limit: int = 256) -> str:
    if len(text) <= limit:
        return text
    return f"{text[:limit]}...[truncated,total={len(text)}]"


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
    def __init__(self):
        self.tag = id(self)
        self.req_seq: Optional[str] = None
        self.session_uuid: Optional[str] = None
        self.peer: Optional[str] = None

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        raise NotImplementedError("Subclasses must implement this method")

    def dump(self) -> str:
        """Return a JSON string representation of this request.

        Subclasses MUST override this method to provide a pure function
        that returns a deterministic JSON string. The output should:
        - Be deterministic (same input -> same output)
        - Not include internal fields (tag, req_seq, session_uuid, peer)
        - Include all relevant business fields
        """
        raise NotImplementedError(
            f"{self.__class__.__name__}.dump() must be implemented by subclass"
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

    def dump(self) -> str:
        return json.dumps(
            {
                "type": self.__class__.__name__,
                "fields": {
                    "command": self.command,
                    "is_gdb_command": self.is_gdb_command,
                },
            },
            sort_keys=True,
            separators=(",", ":"),
        )

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
