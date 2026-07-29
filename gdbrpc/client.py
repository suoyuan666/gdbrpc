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

import logging
import os
import queue
import socket
import sys
import threading
from datetime import datetime
from typing import Dict, Optional, Tuple

import cloudpickle as pickle
from gdbrpc.utils import (
    PacketStatus,
    PostRequest,
    Request,
    Response,
    socket_recv,
    socket_send,
)


class Client:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 20819,
        log_level: int = logging.INFO,
        log_path: Optional[str] = None,
        timeout: float = 300,
    ):
        self._host = host
        self._port = port
        self._timeout = timeout
        self._socket: socket.socket
        self._connected = False
        self._response = queue.Queue()
        self._pending_requests: Dict[int, PostRequest] = {}

        self._logger = logging.getLogger(__name__)
        if not self._logger.hasHandlers():
            self._logger.setLevel(log_level)

            if log_path is None:
                timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
                pid = os.getpid()
                log_path = f"gdbrpc_client-{timestamp}-pid{pid}.log"

            handler = logging.FileHandler(log_path)
            formatter = logging.Formatter("%(asctime)s gdbrpc_client: %(message)s")
            handler.setFormatter(formatter)
            self._logger.addHandler(handler)

    def connect(self):
        try:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.connect((self._host, self._port))
            self._connected = True
            logging.info(f"Connected to GDB server at {self._host}:{self._port}")

            threading.Thread(
                target=self._listen_responses, daemon=True, args=(self._socket,)
            ).start()

            return True
        except Exception as e:
            logging.error(f"Failed to connect {self._host}:{self._port}: {e}")
            return False

    def _listen_responses(self, socket: socket.socket):
        try:
            while self._connected:
                data: Tuple[Response, PacketStatus] = pickle.loads(
                    socket_recv(socket, self._logger)
                )
                response, status = data
                self._logger.debug(
                    f"Received response for request ID {response.tag}, current queue: {self._pending_requests}"
                )

                if status == PacketStatus.PYTHON_VERSION_MISMATCH:
                    response.payload += f"\nclient python version: {sys.version}"
                    self._response.put(response.payload)
                elif (
                    status == PacketStatus.HAS_CALLBACK
                    and response.tag in self._pending_requests
                ):
                    self._logger.debug(f"Handling callback for request ID {status}")
                    callback = self._pending_requests.get(response.tag)

                    assert isinstance(callback, PostRequest)

                    try:
                        callback(response.payload)
                    except Exception as e:
                        self._logger.error(
                            f"Error in callback {callback}, which raised an exception: {e}"
                        )
                    finally:
                        callback.finish.set()
                else:
                    self._response.put(response)
                    self._logger.debug(
                        f"Response for request ID {response.tag} put into response queue"
                    )
                self._logger.info(
                    f"Received data: {response.payload}. from {self._host}:{self._port}"
                )
        except ConnectionError:
            self._logger.info("Connection closed by server")
            self.disconnect()
        except Exception as e:
            self._logger.error(f"Error receiving data: {e}")
            self.disconnect()

    def disconnect(self):
        self._connected = False
        self._logger.info("Disconnecting...")
        if self._socket:
            try:
                self._socket.close()
                self._logger.info("Socket closed successfully.")
            except Exception as e:
                self._logger.error(f"Error closing socket: {e}")
        self._logger.info("Disconnected")

    def no_pending_requests(self) -> bool:
        return len(self._pending_requests) == 0

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

        if post_request is not None:
            self._logger.debug(f"Registering callback for request ID {request.tag}")
            self._pending_requests[request.tag] = post_request
            payload = (request, PacketStatus.HAS_CALLBACK)
        else:
            self._logger.debug(f"No callback for request ID {request.tag}")
            payload = (request, PacketStatus.NO_CALLBACK)

        self._logger.debug(f"Sending request: {request} to {self._host}:{self._port}")

        socket_send(self._socket, pickle.dumps(payload), self._logger)

        try:
            rs = self._response.get(
                timeout=self._timeout if timeout is None else timeout
            )
        except queue.Empty:
            self._logger.error("Request timed out")
            raise TimeoutError("Request timed out")

        return rs.payload
