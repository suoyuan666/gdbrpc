############################################################################
# gdbrpc/tests/test_cli.py
#
# SPDX-License-Identifier: Apache-2.0
############################################################################

import logging
import os
import pickle
import queue
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

from gdbrpc import cli
from gdbrpc.client import Client
from gdbrpc.utils import GdbCommandBatch, PythonExec, RemoteError, ShellExec


class FakeClient:
    instances = []

    def __init__(self, host, port, **kwargs):
        self.host = host
        self.port = port
        self.connected = False
        self.disconnected = False
        self.request = None
        self.callback = None
        self.instances.append(self)

    def connect(self):
        self.connected = True
        return True

    def call(self, request, callback=None, timeout=None):
        self.request = request
        self.callback = callback
        if callback is not None:
            callback("callback-result")
            callback.finish.set()
            return request.tag
        return "command-result"

    def disconnect(self):
        self.disconnected = True


class TestClientDisconnect(unittest.TestCase):
    @mock.patch("gdbrpc.client.socket_send")
    def test_disconnect_wakes_call_waiting_for_response(self, socket_send):
        client = Client(log_level=logging.ERROR, log_path=os.devnull)
        client._connected = True
        client._session_uuid = "test-session"
        client._local_peer = "127.0.0.1:12345"
        client._socket = mock.Mock()
        result = []

        def call():
            try:
                client.call(ShellExec("bt"), timeout=1)
            except Exception as error:
                result.append(error)

        thread = threading.Thread(target=call)
        thread.start()
        time.sleep(0.01)
        client.disconnect()
        thread.join(timeout=1)

        socket_send.assert_called_once()
        self.assertFalse(thread.is_alive())
        self.assertIsInstance(result[0], RemoteError)
        self.assertEqual(result[0].error_type, "ConnectionError")

    @mock.patch("gdbrpc.client.socket_send")
    def test_disconnect_wakes_all_waiting_calls(self, socket_send):
        client = Client(log_level=logging.ERROR, log_path=os.devnull)
        client._connected = True
        client._session_uuid = "test-session"
        client._local_peer = "127.0.0.1:12345"
        client._socket = mock.Mock()
        errors = []

        def call():
            try:
                client.call(ShellExec("bt"), timeout=1)
            except Exception as error:
                errors.append(error)

        threads = [threading.Thread(target=call) for _ in range(2)]
        for thread in threads:
            thread.start()
        time.sleep(0.01)
        client.disconnect()
        for thread in threads:
            thread.join(timeout=1)

        self.assertEqual(socket_send.call_count, 2)
        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(len(errors), 2)
        self.assertTrue(all(isinstance(error, RemoteError) for error in errors))

    def test_normal_disconnect_does_not_leave_stale_response(self):
        client = Client(log_level=logging.ERROR, log_path=os.devnull)
        client._connected = True
        client._session_uuid = "test-session"
        client._local_peer = "127.0.0.1:12345"
        client._socket = mock.Mock()

        client.disconnect()

        self.assertTrue(client._response.empty())


class TestPythonExec(unittest.TestCase):
    def test_remote_error_is_pickle_safe(self):
        error = pickle.loads(pickle.dumps(RemoteError("ValueError", "bad script")))
        self.assertEqual(str(error), "ValueError: bad script")

    def test_sync_script_returns_output(self):
        result = queue.Queue()
        PythonExec('print("script-result")', "script.py")(result)
        self.assertEqual(result.get_nowait(), "script-result\n")

    def test_wait_script_emits_result(self):
        result = queue.Queue()
        PythonExec('emit({"event": "stop"})', "wait.py", wait=True)(result)
        self.assertEqual(result.get_nowait(), {"event": "stop"})

    def test_script_error_is_structured(self):
        result = queue.Queue()
        PythonExec('raise ValueError("bad script")', "bad.py")(result)
        error = result.get_nowait()
        self.assertIsInstance(error, RemoteError)
        self.assertEqual(str(error), "ValueError: bad script")


class TestCLI(unittest.TestCase):
    def setUp(self):
        FakeClient.instances.clear()

    @mock.patch("gdbrpc.cli.Client", FakeClient)
    def test_run_commands_connects_once_and_executes_one_batch(self):
        commands = ["help", "bt", "info registers"]
        results = [
            {
                "index": index,
                "command": command,
                "output": command + "\n",
                "error": None,
            }
            for index, command in enumerate(commands, 1)
        ]

        def call(client, request, callback=None, timeout=None):
            client.request = request
            return results

        with mock.patch.object(FakeClient, "call", call):
            with mock.patch("gdbrpc.cli.print_result") as output:
                status = cli.run_commands("127.0.0.1", 20819, commands, 5)

        client = FakeClient.instances[-1]
        self.assertEqual(status, 0)
        self.assertTrue(client.connected)
        self.assertTrue(client.disconnected)
        self.assertIsInstance(client.request, GdbCommandBatch)
        self.assertEqual(client.request.commands, commands)
        self.assertEqual(output.call_count, 3)

    @mock.patch("gdbrpc.cli.Client", FakeClient)
    def test_run_commands_continue_after_error(self):
        commands = ["help", "bt", "info registers"]
        results = [
            {"index": 1, "command": "help", "output": "help output\n", "error": None},
            {"index": 2, "command": "bt", "output": "", "error": "error: No stack."},
            {
                "index": 3,
                "command": "info registers",
                "output": "registers\n",
                "error": None,
            },
        ]
        with mock.patch.object(FakeClient, "call", return_value=results):
            status = cli.run_commands("127.0.0.1", 20819, commands, 5)

        self.assertEqual(status, 1)

    @mock.patch("gdbrpc.cli.Client", FakeClient)
    def test_run_script_accepts_inline_source(self):
        with mock.patch("gdbrpc.cli.print_result") as output:
            status = cli.run_script("127.0.0.1", 20819, 'print("inline")', timeout=5)

        client = FakeClient.instances[-1]
        self.assertEqual(status, 0)
        self.assertEqual(client.request.source, 'print("inline")')
        self.assertEqual(client.request.filename, "<command-line>")
        output.assert_called_once_with("command-result")

    @mock.patch("gdbrpc.cli.Client", FakeClient)
    def test_run_script_uploads_local_source(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "inspect.py"
            path.write_text('print("uploaded")\n', encoding="utf-8")
            with mock.patch("gdbrpc.cli.print_result") as output:
                status = cli.run_script("127.0.0.1", 20819, str(path), timeout=5)

        client = FakeClient.instances[-1]
        self.assertEqual(status, 0)
        self.assertTrue(client.disconnected)
        self.assertIsInstance(client.request, PythonExec)
        self.assertEqual(client.request.source, 'print("uploaded")\n')
        self.assertFalse(client.request.wait)
        output.assert_called_once_with("command-result")

    @mock.patch("gdbrpc.cli.Client", FakeClient)
    def test_run_script_waits_for_one_callback(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "wait.py"
            path.write_text('emit("done")\n', encoding="utf-8")
            with mock.patch("gdbrpc.cli.print_result") as output:
                status = cli.run_script(
                    "127.0.0.1", 20819, str(path), wait=True, timeout=5
                )

        client = FakeClient.instances[-1]
        self.assertEqual(status, 0)
        self.assertTrue(client.request.wait)
        self.assertTrue(client.disconnected)
        output.assert_called_once_with("callback-result")

    @mock.patch("gdbrpc.cli.Client", FakeClient)
    def test_run_script_reports_callback_error(self):
        def fail_call(client, request, callback=None, timeout=None):
            client.request = request
            callback.error = RemoteError("ValueError", "bad callback")
            callback.finish.set()
            return request.tag

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "wait.py"
            path.write_text('emit("done")\n', encoding="utf-8")
            with mock.patch.object(FakeClient, "call", fail_call):
                status = cli.run_script(
                    "127.0.0.1", 20819, str(path), wait=True, timeout=5
                )

        self.assertEqual(status, 1)

    @mock.patch("gdbrpc.cli.Client", FakeClient)
    def test_run_script_times_out_waiting_for_callback(self):
        def no_callback(client, request, callback=None, timeout=None):
            client.request = request
            return request.tag

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "wait.py"
            path.write_text("pass\n", encoding="utf-8")
            with mock.patch.object(FakeClient, "call", no_callback):
                status = cli.run_script(
                    "127.0.0.1", 20819, str(path), wait=True, timeout=0
                )

        self.assertEqual(status, 124)


if __name__ == "__main__":
    unittest.main()
