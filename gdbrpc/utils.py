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

import logging
import queue
import socket
import struct
import subprocess
import threading
from enum import IntEnum
from typing import Any


def recv_all(connection: socket.socket, length: int, logger: logging.Logger) -> bytes:
    data = b""
    while len(data) < length:
        chunk = connection.recv(length - len(data))
        if not chunk:
            logger.error("Socket connection broken during receive")
            raise ConnectionError("Socket connection broken during receive")
        data += chunk
    return data


def socket_recv(connection: socket.socket, logger: logging.Logger) -> bytes:
    length_data = recv_all(connection, 4, logger)
    # https://docs.python.org/3.10/library/struct.html#format-characters
    data_length = struct.unpack("!I", length_data)[0]
    logger.debug(f"Expecting to receive {data_length} bytes")

    return recv_all(connection, data_length, logger)


def send_all(connection: socket.socket, data: bytes, logger: logging.Logger) -> None:
    total_sent = 0
    while total_sent < len(data):
        sent = connection.send(data[total_sent:])
        if sent == 0:
            logger.error("Socket connection broken during send")
            raise RuntimeError("Socket connection broken")
        total_sent += sent


def socket_send(connection: socket.socket, data: bytes, logger: logging.Logger) -> None:
    try:
        data_length = len(data)
        length_prefix = struct.pack("!I", data_length)
        logger.debug(f"Sending {data_length} bytes")

        send_all(connection, length_prefix, logger)
        send_all(connection, data, logger)

    except Exception as e:
        logger.error(f"Error sending response: {e}")
        raise


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

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        raise NotImplementedError("Subclasses must implement this method")


class PostRequest(Request):
    def __init__(self):
        super().__init__()
        self.finish = threading.Event()

    def __call__(self, argument: Any):
        raise NotImplementedError("Subclasses must implement this method")


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
