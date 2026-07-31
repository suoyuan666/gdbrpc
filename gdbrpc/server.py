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

import json
import logging
import os
import queue
import socket
import sys
import threading
import time
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import cloudpickle as pickle
import gdb
from gdbrpc.utils import (
    FRAME_HEADER_SIZE,
    EventType,
    PacketStatus,
    Request,
    RequestContext,
    Response,
    format_endpoint,
    socket_recv,
    socket_send,
    truncate_text,
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
        self._state_lock = threading.Lock()
        self._pending_requests: Dict[int, Request] = {}
        self._pending_callbacks: Dict[int, str] = {}
        self._sessions: Dict[Tuple[str, int], str] = {}

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

    def _log_event(
        self,
        address: Tuple[str, int],
        event: EventType,
        request: Optional[Request] = None,
        **fields,
    ) -> None:
        session_uuid = request.session_uuid if request is not None else None
        if session_uuid is None:
            session_uuid = self._sessions.get(address)
        parts = [f"peer={format_endpoint(address)}"]
        parts.append(f"session={session_uuid or 'unknown'}")
        if request is not None:
            parts.append(f"req={request.req_seq or 'unknown'}")
        parts.append(f"event={event.value}")
        for key, value in fields.items():
            if value is None:
                continue
            parts.append(f"{key}={value}")
        self._logger.info(" ".join(parts))

    def _payload_text(self, payload: Any) -> str:
        if isinstance(payload, str):
            return json.dumps(truncate_text(payload))
        return json.dumps(truncate_text(str(payload)))

    def _register_request(
        self, address: Tuple[str, int], request: Request, callback_dump: Optional[str]
    ):
        with self._state_lock:
            self._pending_requests[request.tag] = request
            if callback_dump is not None:
                self._pending_callbacks[request.tag] = callback_dump
            if request.session_uuid is not None:
                self._sessions[address] = request.session_uuid

    def _complete_request(self, request: Request):
        with self._state_lock:
            self._pending_requests.pop(request.tag, None)
            self._pending_callbacks.pop(request.tag, None)

    def _counts(self) -> Tuple[int, int]:
        with self._state_lock:
            return len(self._pending_requests), len(self._pending_callbacks)

    def _unpack_payload(self, payload: Any) -> RequestContext:
        callback_dump: Optional[str] = None
        callback_type: Optional[str] = None
        if isinstance(payload, dict):
            request = payload["request"]
            status = payload["status"]
            callback_type = payload.get("callback_type")
            callback_dump = payload.get("callback_dump")
        elif isinstance(payload, tuple):
            if len(payload) == 3:
                request, status, callback_dump = payload
            else:
                request, status = payload
        else:
            raise TypeError(f"Unsupported payload type: {type(payload)!r}")

        if not isinstance(status, PacketStatus):
            status = PacketStatus(status)
        assert isinstance(request, Request)
        return RequestContext(request, status, callback_type, callback_dump)

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

                self._log_event(address, EventType.CONNECT)

                GdbThread(
                    target=self._process_requests,
                    args=(client, address),
                    daemon=True,
                ).start()

            except Exception as e:
                if self.running:
                    self._logger.exception(f"Error accepting connection: {e}")

    def _process_requests_core(
        self,
        client: socket.socket,
        address: Tuple[str, int],
        ctx: RequestContext,
    ) -> None:
        started_at = time.monotonic()
        request, status = ctx.request, ctx.status
        callback_type, callback_dump = ctx.callback_type, ctx.callback_dump
        try:
            async_exec = AsyncExec(request, timeout=self._timeout)

            # https://sourceware.org/gdb/current/onlinedocs/gdb.html/Threading-in-GDB.html
            # gdb.post_event is thread-safe, unlike gdb.execute.
            # gdb.post_event provides an ability to run any callable objects in the gdb main thread.
            gdb.post_event(async_exec)

            self._log_event(
                address,
                EventType.DISPATCH,
                request,
                type=request.__class__.__name__,
            )

            if status == PacketStatus.HAS_CALLBACK:
                self._log_event(
                    address,
                    EventType.REPLY,
                    request,
                    stage="ack",
                    status="ok",
                    callback=callback_type,
                    callback_dump=callback_dump,
                )
                ack_bytes = pickle.dumps(
                    (Response(request.tag, request.tag), PacketStatus.NO_CALLBACK)
                )
                socket_send(client, ack_bytes)
                self._log_event(
                    address,
                    EventType.TRANSPORT_SEND_LEN,
                    request,
                    bytes=FRAME_HEADER_SIZE,
                    frame_bytes=len(ack_bytes),
                    target=format_endpoint(address),
                )
                self._log_event(
                    address,
                    EventType.TRANSPORT_SEND_BODY,
                    request,
                    bytes=len(ack_bytes),
                    target=format_endpoint(address),
                )

            message = async_exec.get_result()
            if isinstance(message, Exception):
                message = f"Error: {str(message)}"

            self._log_event(
                address,
                EventType.DONE,
                request,
                stage="result",
                status="ok",
                callback=callback_type,
                callback_dump=callback_dump,
                payload=self._payload_text(message),
            )

            response_bytes = pickle.dumps((Response(request.tag, message), status))
            socket_send(client, response_bytes)
            self._log_event(
                address,
                EventType.TRANSPORT_SEND_LEN,
                request,
                bytes=FRAME_HEADER_SIZE,
                frame_bytes=len(response_bytes),
                target=format_endpoint(address),
            )
            self._log_event(
                address,
                EventType.TRANSPORT_SEND_BODY,
                request,
                bytes=len(response_bytes),
                target=format_endpoint(address),
            )

            self._complete_request(request)

        except TimeoutError:
            elapsed_s = time.monotonic() - started_at
            pending_requests, pending_callbacks = self._counts()
            self._log_event(
                address,
                EventType.TIMEOUT,
                request,
                stage="waiting_result",
                target="async_exec",
                timeout_s=int(self._timeout),
                elapsed_s=f"{elapsed_s:.3f}",
                callback=callback_type,
                callback_dump=callback_dump,
                pending_requests=pending_requests,
                pending_callbacks=pending_callbacks,
                note="AsyncExec did not receive result from gdb main thread",
            )
            error_msg = (
                f"Timeout after {elapsed_s:.1f}s: AsyncExec did not receive result"
            )
            try:
                error_bytes = pickle.dumps((Response(request.tag, error_msg), status))
                socket_send(client, error_bytes)
            except Exception:
                # Avoid stderr output in GDB environment
                pass
            self._complete_request(request)
        except Exception as e:
            # Avoid stderr output (traceback.print_exc); it would leak
            # into concurrent gdb.execute(..., to_string=True) capture
            # when the server runs inside GDB.
            self._log_event(
                address,
                EventType.ERROR,
                request,
                stage="dispatch",
                status="error",
                error=self._payload_text(str(e)),
                callback=callback_type,
                callback_dump=callback_dump,
            )
            try:
                error_msg = f"Error: {str(e)}"
                error_bytes = pickle.dumps((Response(request.tag, error_msg), status))
                socket_send(client, error_bytes)
            except Exception:
                # Avoid stderr output in GDB environment
                pass
            self._complete_request(request)

    def _process_requests(self, client: socket.socket, address: Tuple[str, int]):
        while self.running:
            try:
                try:
                    data_bytes = socket_recv(client)
                    payload = pickle.loads(data_bytes)
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
                    socket_send(client, response)
                    continue

                ctx = self._unpack_payload(payload)
                self._register_request(address, ctx.request, ctx.callback_dump)

                self._log_event(
                    address,
                    EventType.TRANSPORT_RECV_LEN,
                    ctx.request,
                    bytes=FRAME_HEADER_SIZE,
                    frame_bytes=len(data_bytes),
                    source=format_endpoint(address),
                )
                self._log_event(
                    address,
                    EventType.TRANSPORT_RECV_BODY,
                    ctx.request,
                    bytes=len(data_bytes),
                    source=format_endpoint(address),
                )
                self._log_event(
                    address,
                    EventType.RECV,
                    ctx.request,
                    type=ctx.request.__class__.__name__,
                    dump=ctx.request.dump(),
                    **(
                        {
                            "callback": ctx.callback_type,
                            "callback_dump": ctx.callback_dump,
                        }
                        if ctx.callback_dump is not None
                        else {}
                    ),
                )

                GdbThread(
                    target=self._process_requests_core,
                    args=(client, address, ctx),
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
            self._log_event(address, EventType.DISCONNECT)
            with self.clients_lock:
                self.clients.pop(address, None)
            with self._state_lock:
                self._sessions.pop(address, None)
        except Exception as e:
            self._logger.error(f"Error closing client socket {address}: {e}")

    def stop(self):
        with self.clients_lock:
            for address, client in self.clients.items():
                try:
                    client.close()
                    self._log_event(address, EventType.DISCONNECT)
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
