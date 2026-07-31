#!/usr/bin/python3
############################################################################
# gdbrpc/tests/stresstest.py
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
import random
import sys
import threading
import time
import unittest
from typing import List

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import gdbrpc  # noqa: E402

gdb_list = ["bt", "gcore", "info registers", "info threads"]

nxgdb_list = [
    "dump ram",
    "info nxthread",
    "memfind 0x12341234",
    "nxgcore  -p arm-none-eabi-",
    "ps",
    "source tests/runner.py",
    "stack-usage",
]

command_list = nxgdb_list + gdb_list


class ReturnStrValue(gdbrpc.Request):
    def __init__(self):
        super().__init__()

    def dump(self) -> str:
        return json.dumps(
            {"type": self.__class__.__name__, "fields": {}},
            sort_keys=True,
            separators=(",", ":"),
        )

    def __call__(self, event: queue.Queue):
        import nxgdb

        heaps = nxgdb.mm.get_heaps()
        event.put(str(heaps))


class BaseCase(unittest.TestCase):
    """Base"""

    def verify_registers(self, start_registers: str, end_registers: str):
        self.assertNotIn("thread is running", start_registers.lower())
        self.assertNotIn("thread is running", end_registers.lower())
        for start, end in zip(start_registers.splitlines(), end_registers.splitlines()):
            self.assertEqual(start, end)

    def random_continue_and_interrupt(self, client: gdbrpc.Client):
        self.assertEqual(client.call(gdbrpc.ShellExec("interrupt")), "")

        self.assertEqual(client.call(gdbrpc.ShellExec("nxcontinue")), "")
        sleep_time = random.uniform(3, 6)
        time.sleep(sleep_time)
        self.assertEqual(client.call(gdbrpc.ShellExec("interrupt")), "")


class TestClient(BaseCase):
    """Test cases for gdbrpc Client class."""

    def test_client_initialization(self):
        """Test client initialization with default and custom parameters."""
        client = gdbrpc.Client()
        self.assertEqual(client._host, "localhost")
        self.assertEqual(client._port, 20819)
        self.assertFalse(client._connected)

    def test_client_call(self):
        client = gdbrpc.Client()
        self.assertTrue(client.connect())
        self.assertTrue(isinstance(client.call(ReturnStrValue()), str))
        client.disconnect()

    def test_send_continue(self):
        client = gdbrpc.Client()
        self.assertTrue(client.connect())

        self.assertEqual(client.call(gdbrpc.ShellExec("interrupt")), "")

        self.assertEqual(client.call(gdbrpc.ShellExec("nxcontinue")), "")
        sleep_time = random.uniform(3, 6)
        time.sleep(sleep_time)
        self.assertEqual(client.call(gdbrpc.ShellExec("interrupt")), "")

        client.disconnect()

    def test_send_interrupt(self):
        client = gdbrpc.Client()
        self.assertTrue(client.connect())

        for _ in range(15):
            self.random_continue_and_interrupt(client)

        client.disconnect()


class TestSendNxGDB(BaseCase):
    def run_command(self, client: gdbrpc.Client, command: str):
        start_registers: str = client.call(gdbrpc.ShellExec("info registers"))
        client.call(gdbrpc.ShellExec(command))
        end_registers: str = client.call(gdbrpc.ShellExec("info registers"))
        self.verify_registers(start_registers, end_registers)

    def send_commands(self, client: gdbrpc.Client):
        for _ in range(5):
            self.run_command(client, random.choice(command_list))

    def test_send_command(self):
        client = gdbrpc.Client(logLevel=logging.DEBUG)
        self.assertTrue(client.connect())

        self.random_continue_and_interrupt(client)

        for command in command_list:
            self.run_command(client, command)

        client.disconnect()

        for _ in range(5):

            client_total = 60
            clients: List[gdbrpc.Client] = [
                gdbrpc.Client() for _ in range(client_total)
            ]
            for client in clients:
                self.assertTrue(client.connect())

            threads: List[threading.Thread] = []
            for client in clients:
                thread = threading.Thread(target=self.send_commands, args=(client,))
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()

            self.random_continue_and_interrupt(client)

            for client in clients:
                client.disconnect()


if __name__ == "__main__":
    unittest.main()
