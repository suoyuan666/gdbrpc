#!/usr/bin/python3
############################################################################
# gdbrpc/__main__.py
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

import argparse


def main():
    import gdbrpc
    from gdbrpc.cli import run_commands, run_script

    parser = argparse.ArgumentParser(description="GDB Socket Client")
    parser.add_argument("--host", default="localhost", help="Server host")
    parser.add_argument("--port", type=int, default=20819, help="Server port")
    parser.add_argument(
        "--timeout", type=float, default=300, help="RPC timeout in seconds"
    )
    command = parser.add_mutually_exclusive_group()
    command.add_argument(
        "-c",
        "--command",
        action="append",
        help="complete GDB command unit to execute; may be repeated",
    )
    command.add_argument(
        "-f",
        "--file",
        help="local Python file or inline Python source to execute in GDB",
    )
    parser.add_argument(
        "--wait", action="store_true", help="wait for the Python file to call emit()"
    )

    args = parser.parse_args()
    if args.wait and not args.file:
        parser.error("--wait requires -f/--file")

    if args.command is not None:
        return run_commands(args.host, args.port, args.command, args.timeout)
    if args.file is not None:
        return run_script(args.host, args.port, args.file, args.wait, args.timeout)

    client = gdbrpc.ClientCLI(args.host, args.port)
    client.start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
