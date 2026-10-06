# gdbrpc

🌐 **Languages**: [English](README.md) | [中文](README.zh-CN.md)

A Python-based RPC (Remote Procedure Call) framework for GDB (GNU Debugger) that enables programmatic control and automation of debugging sessions.

**Table of Contents**

---

- [gdbrpc](#gdbrpc)
    - [Overview](#overview)
    - [Features](#features)
    - [Installation](#installation)
        - [From PyPI](#from-pypi)
        - [From Source](#from-source)
    - [Requirements](#requirements)
    - [Quick Start](#quick-start)
        - [Starting the GDB Server](#starting-the-gdb-server)
        - [Using the Python Client](#using-the-python-client)
        - [Using the Interactive CLI](#using-the-interactive-cli)
    - [TODO](#todo)
    - [Architecture](#architecture)
        - [Components](#components)
        - [Communication Flow](#communication-flow)
    - [API Reference](#api-reference)
        - [Client](#client)
        - [Request Classes](#request-classes)
    - [Configuration](#configuration)
        - [Server Configuration](#server-configuration)
        - [Client Configuration](#client-configuration)
    - [Troubleshooting](#troubleshooting)
    - [Contributing](#contributing)
    - [License](#license)
    - [Related Projects](#related-projects)
    - [Support](#support)

## Overview

`gdbrpc` provides a client-server architecture that allows you to control GDB instances remotely through a simple Python API. It's designed to be framework-agnostic and can be used with any GDB-compatible debugger, not limited to any specific operating system or embedded platform.

## Features

- **Remote GDB Control**: Execute GDB commands remotely via socket communication
- **Bidirectional Communication**: Client-server architecture with full duplex support
- **Command Serialization**: Uses cloudpickle for robust serialization of Python objects
- **Interactive CLI**: Built-in command-line interface for quick debugging sessions
- **Extensible**: Easy to integrate into custom debugging workflows and automation scripts

## Installation

### From PyPI

```bash
pip install gdbrpc
```

### From Source

```bash
cd gdbrpc
pip install -e .
```

## Requirements

- Python >= 3.10
- GDB with Python support
- cloudpickle >= 0.0.0

## Quick Start

### Starting the GDB Server

Within a GDB session, use the GDB commands (after importing gdbrpc):

```gdb
(gdb) py import gdbrpc
(gdb) gdbrpc start
(gdb) gdbrpc start --port 20820 --host 0.0.0.0
(gdb) gdbrpc status
(gdb) gdbrpc stop
```

### Using the Python Client

```python
from gdbrpc import Client
from gdbrpc.utils import ShellExec

# Create and connect to the GDB server
client = Client(host="localhost", port=20819)
client.connect()

# Execute GDB commands
response = client.call(ShellExec("info threads"))
print(response)

# Get backtrace
bt = client.call(ShellExec("backtrace"))
print(bt)

# Evaluate expressions
result = client.call(ShellExec("print my_variable"))
print(result)

# Execute shell commands (prefix with !)
output = client.call(ShellExec("!ls -la"))
print(output)

# Close connection
client.disconnect()
```

### Using the CLI

Load the current source checkout in GDB with the repository initializer:

```gdb
source /path/to/gdbrpc/gdbinit.py
gdbrpc start --host 127.0.0.1 --port 20819
```

Run the module or installed `gdbrpc` command without an action to open the
interactive client:

```bash
python3 -m gdbrpc --host localhost --port 20819
gdbrpc --host localhost --port 20819
```

For automation, repeat `-c` to send complete GDB command units in one RPC.
Each unit may itself contain native multiline GDB syntax. A failed unit is
reported, and the remaining units still run:

```bash
gdbrpc --port 20819 -c 'help' -c 'bt' -c 'info registers'
gdbrpc --port 20819 \
  -c $'define dump_state\n  bt\n  info registers\nend' \
  -c 'dump_state'
```

`-f` accepts either a local Python file or inline Python source. An existing
file is read locally; otherwise the argument is compiled as source in GDB:

```bash
gdbrpc --port 20819 -f inspect.py
gdbrpc --port 20819 -f 'import gdb; print(gdb.newest_frame().name())'
gdbrpc --port 20819 -f $'import gdb\nprint(gdb.selected_thread())'
```

Add `--wait` when the script registers a GDB callback. The CLI remains
connected until the script calls the injected `emit(value)` function once:

```python
# wait_stop.py
import gdb


def on_stop(event):
    gdb.events.stop.disconnect(on_stop)
    emit(str(event))


gdb.events.stop.connect(on_stop)
```

```bash
gdbrpc --port 20819 -f wait_stop.py --wait
```

A waiting script must unregister its GDB callback before calling `emit`.
One CLI invocation receives one emitted value; continuous event streaming is
not supported.

## TODO

- [ ] make the CLI provide the same experience as the gdb CLI
    - [ ] auto-completion
    - [ ] command history reading
- [ ] improve network transmission
    - [ ] improving security during deserialization

## Architecture

### Components

- **Server**: Runs inside GDB process, listens for incoming connections
- **Client**: Python client that connects to the server and sends commands
- **CLI**: Interactive command-line interface built on top of the client
- **Protocol**: Custom protocol for request/response communication using cloudpickle

### Communication Flow

```
┌─────────────┐         Socket         ┌─────────────┐
│   Client    │◄──────────────────────►│   Server    │
│  (Python)   │    (Port 20819)        │  (In GDB)   │
└─────────────┘                        └─────────────┘
      │                                       │
      │ Send Request                          │
      │──────────────────────────────────────►│
      │                                       │ Execute Command
      │                                       │ in GDB Context
      │                            Response   │
      │◄──────────────────────────────────────│
      │                                       │
```

## API Reference

### Client

#### `Client(host="localhost", port=20819, logLevel=logging.INFO)`

Create a new client instance.

**Parameters:**
- `host` (str): Server hostname or IP address (default: "localhost")
- `port` (int): Server port number (default: 20819)
- `logLevel` (int): Logging level (default: logging.INFO)

#### `connect() -> bool`

Establish connection to the GDB server.

**Returns:** `True` if connection successful, `False` otherwise

#### `call(request: Request, post_request: Optional[PostRequest] = None, timeout: float = 300) -> Any`

Send a request to the GDB server and receive response.

**Parameters:**
- `request` (Request): Request object to send (typically `ShellExec` for executing commands)
- `post_request` (Optional[PostRequest]): Optional callback request for async handling
- `timeout` (float): Request timeout in seconds (default: 300)

**Returns:** Response payload from the server

**Example:**
```python
from gdbrpc import Client
from gdbrpc.utils import ShellExec

client = Client("localhost", 20819)
client.connect()

# Execute GDB command
result = client.call(ShellExec("info threads"))
print(result)

# Execute shell command (prefix with !)
result = client.call(ShellExec("!ls -la"))
print(result)
```

#### `disconnect()`

Close the connection to the server and cleanup resources.

### Request Classes

#### `ShellExec(command: str)`

Request to execute a GDB command or shell command on the server.

**Parameters:**
- `command` (str): Command to execute
  - GDB commands: `"info threads"`, `"backtrace"`, `"print variable"`
  - Shell commands: prefix with `!` or `shell`, e.g., `"!ls"` or `"shell pwd"`

**Example:**
```python
from gdbrpc.utils import ShellExec

# GDB command
gdb_request = ShellExec("backtrace full")

# Shell command
shell_request = ShellExec("!cat /proc/meminfo")
```

#### `Request`

Base class for all request types. Custom requests can be created by subclassing.

**Methods:**
- `__init__()`: Initializes request with unique tag ID
- `__call__(*args, **kwargs)`: Must be implemented by subclasses

#### `PostRequest`

Base class for requests with callbacks. Used for asynchronous request handling.

**Methods:**
- `__init__()`: Initializes with finish event
- `__call__(argument: Any)`: Must be implemented by subclasses
- `finish` (threading.Event): Event to signal completion

## Configuration

### Server Configuration

The server can be configured when starting:

```python
import logging
import gdbrpc

# Start with debug logging
gdbrpc.start_gdb_socket_server(
    host="0.0.0.0",  # Listen on all interfaces
    port=20819,
    logLevel=logging.DEBUG
)
```

### Client Configuration

```python
import logging
from gdbrpc import Client
from gdbrpc.utils import ShellExec

# Create client with custom log level
client = Client(
    host="localhost",
    port=20819,
    logLevel=logging.DEBUG  # Enable debug logging
)
client.connect()

# Custom timeout for specific requests
result = client.call(
    ShellExec("interrupt"),
    timeout=60  # Wait up to 60 seconds for this command
)
```

## Troubleshooting

### Server won't start

- Ensure GDB has Python support: `gdb --configuration | grep python`
- Check if port is already in use: `netstat -an | grep 20819`
- Verify firewall settings allow the connection

### Connection refused

- Verify server is running: `(gdb) gdbrpc status`
- Check host/port configuration matches between client and server
- Ensure network connectivity between client and server

### Command execution fails

- Verify GDB is in correct state (e.g., program loaded, running)
- Check command syntax is correct for your GDB version
- Review server logs for detailed error messages
- Make sure python version GDB uses is same as client


## Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues for bugs and feature requests.

## License

Licensed under the Apache License, Version 2.0. See the LICENSE file for details.

## Related Projects

- [GDB Python API Documentation](https://sourceware.org/gdb/current/onlinedocs/gdb/Python-API.html)
- [pwndbg](https://github.com/pwndbg/pwndbg) - GDB plugin for exploit development
- [gdb-dashboard](https://github.com/cyrus-and/gdb-dashboard) - Modular GDB dashboard

## Support

For questions and support:
- Open an issue on the project repository
- Consult the GDB Python API documentation
- Review the examples in the `examples/` directory

---

> [!NOTE]
> While gdbrpc was originally developed as part of the NuttX RTOS debugging tools, it is a standalone, general-purpose library that can be used with any GDB debugging session.
