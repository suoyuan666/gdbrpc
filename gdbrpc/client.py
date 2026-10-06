############################################################################
# gdbrpc/client.py
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
import queue
import socket
import sys
import threading
import time
from typing import Any, Dict, Optional, Tuple

import cloudpickle as pickle
from gdbrpc.utils import (
    FRAME_HEADER_SIZE,
    HUMAN_ONLY_FIELDS,
    EventType,
    PacketStatus,
    PostRequest,
    RemoteError,
    Request,
    Response,
    format_endpoint,
    format_human_event,
    make_session_uuid,
    setup_file_logging,
    socket_recv,
    socket_send,
    truncate_text,
)


class Client:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 20819,
        log_level: int = logging.INFO,
        log_path: Optional[str] = None,
        timeout: float = 300,
        human_log_path: Optional[str] = None,
    ):
        self._host = host
        self._port = port
        self._timeout = timeout
        self._socket: socket.socket
        self._connected = False
        self._session_uuid: Optional[str] = None
        self._local_peer: Optional[str] = None
        self._next_req_seq = 0
        self._response = queue.Queue()
        self._response_waiters = 0
        self._pending_requests: Dict[int, Request] = {}
        self._pending_callbacks: Dict[int, PostRequest] = {}
        self._request_lock = threading.Lock()

        self._logger, self._human_logger = setup_file_logging(
            __name__, "gdbrpc_client", log_level, log_path, human_log_path
        )

    def _log_human_event(
        self, event: EventType, request: Optional[Request] = None, **fields
    ) -> None:
        message = format_human_event("client", event, request, **fields)
        if message is not None:
            self._human_logger.info(message)

    def _log_event(
        self, event: EventType, request: Optional[Request] = None, **fields
    ) -> None:
        parts = [f"peer={self._local_peer or 'unknown'}"]
        parts.append(f"session={self._session_uuid or 'unknown'}")
        if request is not None:
            parts.append(f"req={request.req_seq or 'unknown'}")
        parts.append(f"event={event.value}")
        for key, value in fields.items():
            if value is None or key in HUMAN_ONLY_FIELDS:
                continue
            parts.append(f"{key}={value}")
        self._logger.info(" ".join(parts))
        self._log_human_event(event, request, **fields)

    def _payload_text(self, payload: Any, limit: Optional[int] = 256) -> str:
        text = payload if isinstance(payload, str) else str(payload)
        return json.dumps(truncate_text(text, limit))

    def _counts(self) -> Tuple[int, int]:
        with self._request_lock:
            return len(self._pending_requests), len(self._pending_callbacks)

    def connect(self):
        try:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self._socket.connect((self._host, self._port))
            self._connected = True
            self._session_uuid = make_session_uuid()
            self._next_req_seq = 0
            self._local_peer = format_endpoint(self._socket.getsockname())
            self._log_event(
                EventType.CONNECT, target=format_endpoint((self._host, self._port))
            )

            threading.Thread(
                target=self._listen_responses, daemon=True, args=(self._socket,)
            ).start()

            return True
        except Exception as e:
            self._logger.error(f"Failed to connect {self._host}:{self._port}: {e}")
            return False

    def _listen_responses(self, socket: socket.socket):
        try:
            while self._connected:
                data_bytes = socket_recv(socket)
                data: Tuple[Response, PacketStatus] = pickle.loads(data_bytes)
                response, status = data

                with self._request_lock:
                    request = self._pending_requests.get(response.tag)
                    callback = self._pending_callbacks.get(response.tag)

                if request is not None:
                    self._log_event(
                        EventType.TRANSPORT_RECV_LEN,
                        request,
                        bytes=FRAME_HEADER_SIZE,
                        frame_bytes=len(data_bytes),
                        source=format_endpoint((self._host, self._port)),
                    )
                    self._log_event(
                        EventType.TRANSPORT_RECV_BODY,
                        request,
                        bytes=len(data_bytes),
                        source=format_endpoint((self._host, self._port)),
                    )

                if status == PacketStatus.PYTHON_VERSION_MISMATCH:
                    response.payload += f"\nclient python version: {sys.version}"
                    self._response.put(response)
                    self._log_event(
                        EventType.RECV,
                        request,
                        stage="error",
                        status="python_version_mismatch",
                        payload=self._payload_text(response.payload),
                        payload_full=self._payload_text(response.payload, limit=None),
                    )
                    continue

                if status == PacketStatus.HAS_CALLBACK and request is not None:
                    assert isinstance(callback, PostRequest)

                    try:
                        if isinstance(response.payload, RemoteError):
                            callback.error = response.payload
                        else:
                            callback(response.payload)
                        self._log_event(
                            EventType.CALLBACK,
                            request,
                            stage="result",
                            status="ok",
                            callback=f"{callback.__class__.__name__}",
                            callback_dump=callback.callback_dump(),
                            payload=self._payload_text(response.payload),
                            payload_full=self._payload_text(
                                response.payload, limit=None
                            ),
                        )
                    except Exception as e:
                        callback.error = e
                        self._log_event(
                            EventType.ERROR,
                            request,
                            stage="callback",
                            status="error",
                            callback=f"{callback.__class__.__name__}",
                            callback_dump=callback.callback_dump(),
                            error=self._payload_text(str(e)),
                        )
                    finally:
                        callback.finish.set()
                        with self._request_lock:
                            self._pending_callbacks.pop(response.tag, None)
                            self._pending_requests.pop(response.tag, None)
                else:
                    self._response.put(response)
                    self._log_event(
                        EventType.RECV,
                        request,
                        stage="result" if status == PacketStatus.NO_CALLBACK else "ack",
                        status="ok",
                        payload=self._payload_text(response.payload),
                        payload_full=self._payload_text(response.payload, limit=None),
                    )
                    with self._request_lock:
                        # Only pop for a true final result. A NO_CALLBACK reply
                        # with a pending callback is the ACK of a two-step
                        # HAS_CALLBACK request, whose result must match it.
                        if status == PacketStatus.NO_CALLBACK and callback is None:
                            self._pending_requests.pop(response.tag, None)
        except ConnectionError:
            self._logger.info("Connection closed by server")
            self.disconnect()
        except OSError as e:
            if self._connected:
                self._logger.error(f"Error receiving data: {e}")
            else:
                self._logger.debug(f"Socket closed during client shutdown: {e}")
            self.disconnect()
        except Exception as e:
            self._logger.error(f"Error receiving data: {e}")
            self.disconnect()

    def disconnect(self):
        was_connected = self._connected
        self._connected = False
        self._log_event(
            EventType.DISCONNECT, target=format_endpoint((self._host, self._port))
        )
        if hasattr(self, "_socket"):
            try:
                self._socket.close()
            except Exception as e:
                self._logger.error(f"Error closing socket: {e}")
        if was_connected:
            message = "Connection closed before request completed"
            error = ConnectionError(message)
            with self._request_lock:
                for _ in range(self._response_waiters):
                    self._response.put(
                        Response(0, RemoteError("ConnectionError", message))
                    )
                callbacks = list(self._pending_callbacks.values())
                self._pending_callbacks.clear()
                self._pending_requests.clear()
            for callback in callbacks:
                callback.error = error
                callback.finish.set()

    def no_pending_requests(self) -> bool:
        return len(self._pending_requests) == 0 and len(self._pending_callbacks) == 0

    def call(
        self,
        request: Request,
        post_request: Optional[PostRequest] = None,
        timeout: Optional[float] = None,
    ):
        if not self._connected:
            raise ConnectionError("Not connected to server")
        if not isinstance(request, Request):
            raise TypeError("request must be a Request instance")
        if post_request is not None and not isinstance(post_request, PostRequest):
            raise TypeError("post_request must be a PostRequest instance or None")

        if self._session_uuid is None or self._local_peer is None:
            raise ConnectionError("Session is not initialized")

        with self._request_lock:
            self._next_req_seq += 1
            request.req_seq = f"{self._next_req_seq:04d}"
            request.session_uuid = self._session_uuid
            request.peer = self._local_peer
            self._pending_requests[request.tag] = request
            if post_request is not None:
                self._pending_callbacks[request.tag] = post_request

        if post_request is not None:
            callback_type = post_request.__class__.__name__
            callback_dump = post_request.callback_dump()
            payload = {
                "request": request,
                "status": PacketStatus.HAS_CALLBACK,
                "callback_type": callback_type,
                "callback_dump": callback_dump,
            }
        else:
            callback_dump = None
            callback_type = None
            payload = {
                "request": request,
                "status": PacketStatus.NO_CALLBACK,
                "callback_type": None,
                "callback_dump": None,
            }

        self._log_event(
            EventType.SEND,
            request,
            type=request.__class__.__name__,
            dump=request.dump(),
            **(
                {"callback": callback_type, "callback_dump": callback_dump}
                if post_request is not None
                else {}
            ),
            target=format_endpoint((self._host, self._port)),
        )

        data = pickle.dumps(payload)
        socket_send(self._socket, data)
        self._log_event(
            EventType.TRANSPORT_SEND_LEN,
            request,
            bytes=FRAME_HEADER_SIZE,
            frame_bytes=len(data),
            target=format_endpoint((self._host, self._port)),
        )
        self._log_event(
            EventType.TRANSPORT_SEND_BODY,
            request,
            bytes=len(data),
            target=format_endpoint((self._host, self._port)),
        )

        started_at = time.monotonic()
        actual_timeout = self._timeout if timeout is None else timeout

        with self._request_lock:
            self._response_waiters += 1
        try:
            rs = self._response.get(timeout=actual_timeout)
        except queue.Empty:
            elapsed_s = time.monotonic() - started_at
            pending_requests, pending_callbacks = self._counts()

            self._log_event(
                EventType.TIMEOUT,
                request,
                stage="waiting_result",
                target="transport_recv",
                timeout_s=int(actual_timeout),
                elapsed_s=f"{elapsed_s:.3f}",
                dump=request.dump(),
                **(
                    {
                        "callback": callback_type,
                        "callback_dump": callback_dump,
                        "pending_requests": pending_requests,
                        "pending_callbacks": pending_callbacks,
                    }
                    if post_request is not None
                    else {
                        "pending_requests": pending_requests,
                        "pending_callbacks": pending_callbacks,
                    }
                ),
                note="Client did not receive response within timeout",
            )
            raise TimeoutError("Request timed out")
        finally:
            with self._request_lock:
                self._response_waiters -= 1

        if isinstance(rs.payload, RemoteError):
            raise rs.payload
        return rs.payload
