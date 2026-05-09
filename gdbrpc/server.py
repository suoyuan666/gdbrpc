############################################################################
# gdbrpc/server.py
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
import os
import queue
import socket
import sys
import threading
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import cloudpickle as pickle
import gdb
from gdbrpc.utils import (
    PacketStatus,
    Request,
    Response,
    socket_recv,
    socket_send,
)


# Custom thread class for GDB compatibility
# In newer GDB versions (>= 14.1), gdb.Thread is available as a subclass of threading.Thread
# that automatically blocks signals. For older versions, we implement it ourselves.
class GdbThread(threading.Thread):
    """
    A thread class that works seamlessly with GDB's signal handling requirements.
    This is a drop-in replacement for gdb.Thread in newer GDB versions.
    """

    def start(self):
        """Override start to block signals before starting the thread."""
        # Check if gdb.blocked_signals is available (GDB >= 13.1)
        if hasattr(gdb, "blocked_signals"):
            with gdb.blocked_signals():
                super().start()
        else:
            # Fall back to regular threading for older GDB versions
            super().start()


class AsyncExec:
    def __init__(self, request: Request, timeout: float = 300):
        self.request: Request = request
        self._queue = queue.Queue()
        self._timeout = timeout

    def __call__(self):
        self.request(self._queue)

    def get_result(self) -> Any:
        try:
            return self._queue.get(timeout=self._timeout)
        except queue.Empty:
            raise TimeoutError("No result available within the specified timeout")


class Server:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 20819,
        log_level: int = logging.INFO,
        log_path: Optional[str] = None,
        timeout: float = 300,
    ):
        self.port = port
        self.host = host
        self._timeout = timeout
        self.server: socket.socket
        self.running = False
        self.accept_thread: Optional[GdbThread] = None
        self.clients_lock = threading.Lock()
        self.clients: Dict[Tuple[str, int], socket.socket] = {}

        self._logger = logging.getLogger(__name__)
        if not self._logger.hasHandlers():
            self._logger.setLevel(log_level)

            formatter = logging.Formatter("%(asctime)s gdbrpc_server: %(message)s")

            if log_path is None:
                timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
                pid = os.getpid()
                log_path = f"gdbrpc_server-{timestamp}-pid{pid}.log"

            file_handler = logging.FileHandler(log_path)
            file_handler.setFormatter(formatter)

            self._logger.addHandler(file_handler)
            # Note: a stderr StreamHandler is intentionally NOT attached.
            # When this server runs inside GDB, Python's sys.stderr is
            # redirected to GDB's output stream and gets captured by any
            # concurrent `gdb.execute(..., to_string=True)` on the main
            # thread, polluting captured command output (e.g. the netstat
            # test in tests/test_runtime_net.py). Diagnostics remain
            # available through the per-session file handler above.

    def start(self):
        try:
            self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            try:
                self.server.bind((self.host, int(self.port)))
            except Exception as e:
                self._logger.info(
                    f"Error binding to port {self.port}: {e}, so binding to a random port"
                )
                self.server.bind((self.host, 0))
                self.port = self.server.getsockname()[1]

            self.server.listen()
            self.running = True

            self._logger.info(f"GDB Socket Server started on {self.host}:{self.port}")
            print(f"GDB Socket Server started on {self.host}:{self.port}")

            def set_pagination_off():
                gdb.execute("set pagination off ")

            gdb.post_event(set_pagination_off)
            self._logger.info("Set GDB pagination off")

            self.accept_thread = GdbThread(target=self._accept, daemon=True)
            self.accept_thread.start()

        except Exception as e:
            self._logger.error(f"Failed to start GDB Socket Server: {e}")

    def _accept(self):
        while self.running:
            try:
                client, address = self.server.accept()

                with self.clients_lock:
                    self.clients[address] = client

                self._logger.debug(
                    f"Accepted connection from {address}, total clients: {len(self.clients)}"
                )

                GdbThread(
                    target=self._process_requests,
                    args=(client, address),
                    daemon=True,
                ).start()

            except Exception as e:
                if self.running:
                    # Use logger.exception to keep the traceback in the log
                    # file only; printing to stderr would pollute any
                    # concurrent gdb.execute(..., to_string=True) capture.
                    self._logger.exception(f"Error accepting connection: {e}")

    def _process_requests_core(
        self, client: socket.socket, request: Request, status: PacketStatus
    ) -> None:
        try:
            async_exec = AsyncExec(request, timeout=self._timeout)

            # https://sourceware.org/gdb/current/onlinedocs/gdb.html/Threading-in-GDB.html
            # gdb.post_event is thread-safe, unlike gdb.execute.
            # gdb.post_event provides an ability to run any callable objects in the gdb main thread.
            gdb.post_event(async_exec)

            if status == PacketStatus.HAS_CALLBACK:
                self._logger.debug(
                    f"Posted event for callback {status} with {request}, waiting for completion"
                )
            else:
                self._logger.debug(
                    f"Posted event with {request}, waiting for completion"
                )

            # If the request does not has callback, we should send message to client immediately.
            # Because the client call must be not blocked.
            if status == PacketStatus.HAS_CALLBACK:
                socket_send(
                    client,
                    pickle.dumps(
                        (
                            Response(request.tag, request.tag),
                            PacketStatus.NO_CALLBACK,
                        )
                    ),
                    self._logger,
                )

            message = async_exec.get_result()

            if isinstance(message, Exception):
                message = f"Error: {str(message)}"

            if status == PacketStatus.HAS_CALLBACK:
                self._logger.debug(f"Callback {status} with {request} completed")
            else:
                self._logger.debug(f"{request} completed")

            socket_send(
                client,
                pickle.dumps((Response(request.tag, message), status)),
                self._logger,
            )

        except Exception as e:
            # Avoid stderr output (traceback.print_exc); it would leak
            # into concurrent gdb.execute(..., to_string=True) capture
            # when the server runs inside GDB.
            self._logger.exception(f"Error running callback {status}: {e}")

    def _process_requests(self, client: socket.socket, address):
        while self.running:
            try:
                try:
                    data_bytes = socket_recv(client, self._logger)
                    payload: Tuple[Request, PacketStatus] = pickle.loads(data_bytes)
                except (TypeError, ValueError) as e:
                    # cloudpickle needs the same Python version to serialize/deserialize the object.
                    #
                    # Q: Why did we choose cloudpickle instead of pickle?
                    # A: Because standard pickle cannot deserialize objects
                    # that are not defined at the top level of a module.
                    #
                    # But Python standard pickle has backwards compatibility.
                    # So if standard pickle can deserialize an object,
                    # even if the object is not defined at the top level of a module,
                    # we can use standard pickle.
                    message = (
                        f"Error: {str(e)}\n"
                        f"maybe python version mismatch\n"
                        f"server python version: {sys.version}"
                    )
                    response = pickle.dumps(
                        (Response(0, message), PacketStatus.PYTHON_VERSION_MISMATCH)
                    )
                    socket_send(client, response, self._logger)
                    continue

                request, status = payload
                self._logger.info(f"Received request from {address}: {request}")
                assert isinstance(request, Request)

                GdbThread(
                    target=self._process_requests_core,
                    args=(client, request, status),
                    daemon=True,
                ).start()

            except ConnectionError:
                break
            except Exception as e:
                # Avoid stderr output (traceback.print_exc); it would leak
                # into concurrent gdb.execute(..., to_string=True) capture
                # when the server runs inside GDB.
                self._logger.exception(f"Error handling client {address}: {e}")

        try:
            client.close()
            self._logger.info(f"Closed connection from {address}")
            with self.clients_lock:
                if address in self.clients:
                    del self.clients[address]
                    self._logger.debug(
                        f"Removed client {address}, total clients: {len(self.clients)}"
                    )
        except Exception as e:
            self._logger.error(f"Error closing client socket {address}: {e}")

    def stop(self):
        with self.clients_lock:
            for address, client in self.clients.items():
                try:
                    client.close()
                    self._logger.info(f"Closed client connection from {address}")
                except Exception as e:
                    self._logger.error(f"Error closing client socket {address}: {e}")
            self.clients.clear()

        if self.running:
            try:
                self.server.close()
                self.running = False
            except Exception as e:
                self._logger.error(f"Error closing server socket: {e}")

        if self.accept_thread and self.accept_thread.is_alive():
            try:
                self.accept_thread.join(timeout=2.0)
                if self.accept_thread.is_alive():
                    self._logger.warning(
                        "Accept thread did not terminate within timeout"
                    )
            except Exception as e:
                self._logger.error(f"Error waiting for accept thread: {e}")

        self._logger.info("GDB Socket Server stopped")
