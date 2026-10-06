############################################################################
# gdbrpc/tests/test_transport.py
#
# SPDX-License-Identifier: Apache-2.0
############################################################################

import logging
import socket
import struct
import unittest
from unittest import mock

from gdbrpc.client import Client
from gdbrpc.utils import recv_all, socket_send


class TestSocketTransport(unittest.TestCase):
    def test_socket_send_writes_one_complete_frame(self):
        connection = mock.Mock()
        payload = b"payload"

        socket_send(connection, payload)

        connection.sendall.assert_called_once_with(
            struct.pack("!I", len(payload)) + payload
        )

    def test_receive_eof_raises_connection_error(self):
        connection = mock.Mock()
        connection.recv.return_value = b""

        with self.assertRaises(ConnectionError):
            recv_all(connection, 4)

    @mock.patch("gdbrpc.client.threading.Thread")
    @mock.patch("gdbrpc.client.socket.socket")
    def test_client_disables_nagle(self, socket_factory, thread):
        connection = socket_factory.return_value
        connection.getsockname.return_value = ("127.0.0.1", 12345)
        client = Client(log_level=logging.ERROR, log_path="/dev/null")

        self.assertTrue(client.connect())

        connection.setsockopt.assert_called_once_with(
            socket.IPPROTO_TCP, socket.TCP_NODELAY, 1
        )
        connection.connect.assert_called_once_with(("localhost", 20819))
        thread.return_value.start.assert_called_once()


if __name__ == "__main__":
    unittest.main()
