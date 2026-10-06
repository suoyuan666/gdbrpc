############################################################################
# gdbrpc/__init__.py
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

__all__ = [
    "Client",
    "ClientCLI",
    "GdbCommand",
    "GdbCommandBatch",
    "PostRequest",
    "Request",
    "RemoteError",
    "Response",
    "PacketStatus",
    "PythonExec",
    "Server",
    "ShellExec",
]

from .cli import ClientCLI

# Client must be imported first because ClientCLI depends on it
from .client import Client
from .utils import (
    GdbCommand,
    GdbCommandBatch,
    PacketStatus,
    PostRequest,
    PythonExec,
    RemoteError,
    Request,
    Response,
    ShellExec,
)

# Register GDB commands if running inside GDB
try:
    from gdb import COMMAND_USER, Command

    from .commands import StartSocketClient  # noqa: F401
    from .commands import (
        SocketServerStatus,
        StartSocketServer,
        StopSocketServer,
    )

    class GDBRpcPrefix(Command):
        """GDB Remote Protocol related commands prefix"""

        def __init__(self):
            super().__init__("gdbrpc", COMMAND_USER, prefix=True)

    print("Registering gdbrpc commands...")
    GDBRpcPrefix()
    StartSocketServer()
    StopSocketServer()
    SocketServerStatus()
    StartSocketClient()
except ImportError:
    pass

try:
    from ._version import version

    __version__ = version
except ImportError:
    __version__ = "0.0.0.dev0"
