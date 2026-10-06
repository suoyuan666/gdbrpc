############################################################################
# gdbrpc/cli.py
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
import sys
import time
from pathlib import Path
from typing import Any

from gdbrpc.client import Client
from gdbrpc.utils import GdbCommandBatch, PostRequest, PythonExec, ShellExec


class PrintResult(PostRequest):
    def __init__(self):
        super().__init__()
        self.result: Any = None

    def __call__(self, result: Any):
        self.result = result


def print_result(result: Any):
    if result is None:
        return

    if isinstance(result, bytes):
        sys.stdout.buffer.write(result)
        return

    output = result if isinstance(result, str) else repr(result)
    print(output, end="" if output.endswith("\n") else "\n")


def run_commands(
    host: str, port: int, commands: list[str], timeout: float = 300
) -> int:
    client = Client(host, port, log_level=logging.WARNING, log_path=os.devnull)
    if not client.connect():
        print(f"Unable to connect to {host}:{port}", file=sys.stderr)
        return 3

    try:
        results = client.call(GdbCommandBatch(commands), timeout=timeout)
        failed = False
        for result in results:
            print_result(result["output"])
            if result["error"]:
                failed = True
                print(
                    f"command {result['index']} failed: {result['command']}: "
                    f"{result['error']}",
                    file=sys.stderr,
                )
        return 1 if failed else 0
    except TimeoutError as e:
        print(e, file=sys.stderr)
        return 124
    except Exception as e:
        print(e, file=sys.stderr)
        return 1
    finally:
        client.disconnect()


def run_script(
    host: str,
    port: int,
    source_or_path: str,
    wait: bool = False,
    timeout: float = 300,
) -> int:
    path = Path(source_or_path)
    if path.is_file():
        try:
            script = path.read_text(encoding="utf-8")
        except OSError as e:
            print(e, file=sys.stderr)
            return 2
        filename = str(path.resolve())
    else:
        script = source_or_path
        filename = "<command-line>"

    client = Client(host, port, log_level=logging.WARNING, log_path=os.devnull)
    if not client.connect():
        print(f"Unable to connect to {host}:{port}", file=sys.stderr)
        return 3

    try:
        request = PythonExec(script, filename, wait)
        if not wait:
            print_result(client.call(request, timeout=timeout))
            return 0

        callback = PrintResult()
        start = time.monotonic()
        client.call(request, callback, timeout=timeout)
        remaining = max(0, timeout - (time.monotonic() - start))
        if not callback.finish.wait(remaining):
            raise TimeoutError("Callback timed out")
        if callback.error is not None:
            raise callback.error
        print_result(callback.result)
        return 0
    except KeyboardInterrupt:
        return 130
    except TimeoutError as e:
        print(e, file=sys.stderr)
        return 124
    except Exception as e:
        print(e, file=sys.stderr)
        return 1
    finally:
        client.disconnect()


class ClientCLI(Client):
    def __init__(self, host, port):
        super().__init__(host, port, log_level=logging.WARNING)

    def _show_command_help(self):
        print("Welcome to the GDB Remote Protocol Client")
        print("Type `exit` or `quit` to disconnect.")
        print("Type `help` to show this help message.")
        print("If you need `interrupt` command to stop the target, use Ctrl+C.")

    def _loop(self):
        print("gdb> ", end="", flush=True)
        while True:
            try:
                command = input().strip()
                if not command:
                    continue
                if command.lower() in ("exit", "quit"):
                    self.disconnect()
                    break
                if command.lower() == "help":
                    self._show_command_help()
                    print("gdb> ", end="", flush=True)
                    continue
                print(self.call(ShellExec(command)))
                print("gdb> ", end="", flush=True)
            except KeyboardInterrupt:
                print("")
                self.call(ShellExec("interrupt"))
                print("gdb> ", end="", flush=True)
            except EOFError:
                self.disconnect()

    def start(self):
        assert self.connect()
        self._show_command_help()
        self._loop()
