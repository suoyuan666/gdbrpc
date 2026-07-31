#!/usr/bin/python3
############################################################################
# gdbrpc/examples/memleak.py
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
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import gdbrpc  # noqa: E402


class FetchResult(gdbrpc.PostRequest):
    def __init__(self):
        super().__init__()

    def callback_dump(self) -> str:
        return json.dumps(
            {
                "type": self.__class__.__name__,
                "fields": {"purpose": "wait for memleak result"},
            },
            sort_keys=True,
            separators=(",", ":"),
        )

    def __call__(self, result):
        print(result)


class MemLeak(gdbrpc.Request):
    def __init__(self):
        super().__init__()

    def dump(self) -> str:
        return json.dumps(
            {"type": self.__class__.__name__, "fields": {}},
            sort_keys=True,
            separators=(",", ":"),
        )

    def __call__(self, q: queue.Queue):
        import gdb

        q.put(gdb.execute("memleak", to_string=True))


if __name__ == "__main__":

    client = gdbrpc.Client(logLevel=logging.DEBUG)

    assert client.connect()

    bt = FetchResult()
    client.call(MemLeak(), bt)

    print("Waiting for async call to complete...")

    bt.finish.wait()

    print("Async call completed.")

    client.disconnect()
