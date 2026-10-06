# gdbrpc

🌐 **Languages**: [English](README.md) | [中文](README.zh-CN.md)

一个基于 Python 的 RPC 框架，用于和 GDB 远程通信，远程调试 GDB 调试的目标

> [!NOTE]
> 虽然 gdbrpc 最先是为 NuttX RTOS 的调试工具开发，但实际上这是一个通用工具，可以被集成在任何使用了 GDB 的框架中作为新的 GDB 通信方式

**目录**

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

gdbrpc 提供了一个 client-server 架构，允许开发者连接到一个已有的 GDB 调试会话中，同时 gdbrpc 提供了 Python API 用于让开发者在 GDB 上执行任意 Python 代码

## Features

- **远程控制 GDB**: 通过 socket 让 GDB 执行命令
- **GDB CLI**: 内置一个 GDB CLI 接口，用于提供和 GDB CLI 类似的体验
- **扩展性**: 提供了 Python API 用于执行命令，容易集成到 GDB 相关的其他框架中


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

在 GDB 会话中启动 gdbrpc

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

### 使用 CLI

可用仓库根目录的初始化脚本在 GDB 中加载当前源码：

```gdb
source /path/to/gdbrpc/gdbinit.py
gdbrpc start --host 127.0.0.1 --port 20819
```

不指定操作时，模块或安装后的 `gdbrpc` 命令会进入交互模式：

```bash
python3 -m gdbrpc --host localhost --port 20819
gdbrpc --host localhost --port 20819
```

自动化场景可重复使用 `-c`，在一次 RPC 中发送多个完整 GDB 命令单元。每个单元
本身仍可包含 GDB 原生多行语法；某个单元失败时会报告错误并继续执行后续单元：

```bash
gdbrpc --port 20819 -c 'help' -c 'bt' -c 'info registers'
gdbrpc --port 20819 \
  -c $'define dump_state\n  bt\n  info registers\nend' \
  -c 'dump_state'
```

`-f` 既可接收客户端本地 Python 文件，也可直接接收 Python 源码。参数对应现有普通
文件时读取文件内容，否则将参数作为源码在 GDB 主线程执行：

```bash
gdbrpc --port 20819 -f inspect.py
gdbrpc --port 20819 -f 'import gdb; print(gdb.newest_frame().name())'
gdbrpc --port 20819 -f $'import gdb\nprint(gdb.selected_thread())'
```

脚本注册 GDB callback 时增加 `--wait`。CLI 会保持连接，直到脚本调用一次注入的
`emit(value)`：

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

等待脚本必须在调用 `emit` 前注销 GDB callback。一次 CLI 调用只接收一个值，首版
不支持连续事件流。

## TODO

- [ ] 让 CLI 有接近 GDB CLI 的体验
    - [ ] 自动补全
    - [ ] 命令历史记录
- [ ] 改进网络传输
    - [ ]  提升反序列化的安全性


> [!TODO]
> 架构、API 介绍等暂不翻译，为了防止后续 API 变化且 README 没有同步引发开发者的误解问题

## Troubleshooting

### 服务器无法启动

- 确认 GDB 已启用 Python 支持：`gdb --configuration | grep python`
- 检查端口是否已被占用：`netstat -an | grep 20819`
- 确认防火墙设置允许该连接

### 连接被拒绝

- 确认服务器正在运行：`(gdb) gdbrpc status`
- 检查客户端与服务器的主机/端口配置是否一致
- 确保客户端与服务器之间的网络连接正常

### 命令执行失败

- 确认 GDB 当前状态正确（例如程序已加载、正在运行）
- 检查命令语法是否符合当前 GDB 版本
- 查看服务器日志以获取详细错误信息
- 确保 GDB 使用的 Python 版本与客户端一致

## Contributing

欢迎任何贡献！错误报告、功能请求或者是代码提交等都可以提🤗

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
