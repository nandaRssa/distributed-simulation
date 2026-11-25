"""
Unit Test: test_communication.py
Menguji Message Framing (Length-Prefixed Framing), ketahanan terhadap fragmentasi paket TCP,
pengiriman payload besar, serta fungsionalitas MasterServer dalam menangani pertukaran pesan JSON.
"""

import sys
import os
import unittest
import socket
import threading
import time
from typing import Dict, Any

# Menambahkan direktori project root ke sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from shared.protocol import MessageType, ProtocolMessage
from shared.message_framing import send_framed_message, recv_framed_message, FramingError
from master.communication import MasterServer


class TestCommunication(unittest.TestCase):

    def test_message_framing_roundtrip(self):
        """Uji pengiriman dan penerimaan framed message melalui socket pair."""
        server_sock, client_sock = socket.socketpair()

        msg = ProtocolMessage.create(MessageType.REGISTER, {"worker_id": "W-1", "cores": 4})
        send_framed_message(client_sock, msg)

        received_msg = recv_framed_message(server_sock)
        self.assertEqual(received_msg, msg)

        server_sock.close()
        client_sock.close()

    def test_large_payload_framing(self):
        """Uji pengiriman payload besar (misal array lintasan SIR ribuan elemen)."""
        server_sock, client_sock = socket.socketpair()

        large_array = [float(i) for i in range(20000)]
        msg = ProtocolMessage.create(MessageType.RESULT, {"data": large_array})

        send_framed_message(client_sock, msg)
        received_msg = recv_framed_message(server_sock)

        self.assertEqual(received_msg["type"], MessageType.RESULT.value)
        self.assertEqual(len(received_msg["payload"]["data"]), 20000)

        server_sock.close()
        client_sock.close()

    def test_back_to_back_messages(self):
        """Uji pengiriman beberapa pesan berturut-turut tanpa jeda (anti-stuck framing)."""
        server_sock, client_sock = socket.socketpair()

        messages = [
            ProtocolMessage.create(MessageType.STATUS, {"status": "BUSY", "idx": i})
            for i in range(10)
        ]

        # Kirim 10 pesan sekaligus
        for m in messages:
            send_framed_message(client_sock, m)

        # Terima 10 pesan
        for i in range(10):
            received = recv_framed_message(server_sock)
            self.assertEqual(received["payload"]["idx"], i)

        server_sock.close()
        client_sock.close()

    def test_master_server_client_interaction(self):
        """Uji integrasi MasterServer: client register, receive ACK, invalid msg handling, and shutdown."""
        # Cari port bebas
        temp_s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        temp_s.bind(("127.0.0.1", 0))
        free_port = temp_s.getsockname()[1]
        temp_s.close()

        server = MasterServer(host="127.0.0.1", port=free_port)

        # Register handler
        def handle_register(sock, addr, msg):
            w_id = msg["payload"].get("worker_id")
            reply = ProtocolMessage.create(
                MessageType.REGISTER_ACK,
                {"status": "ACCEPTED", "worker_id": w_id}
            )
            server.send_message(sock, reply)

        server.register_handler(MessageType.REGISTER, handle_register)
        server.start()
        time.sleep(0.05)  # Tunggu server aktif

        # Hubungkan client socket
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(("127.0.0.1", free_port))

        # 1. Kirim pesan REGISTER
        reg_msg = ProtocolMessage.create(MessageType.REGISTER, {"worker_id": "WORKER-01"})
        send_framed_message(client, reg_msg)

        ack = recv_framed_message(client)
        self.assertEqual(ack["type"], MessageType.REGISTER_ACK.value)
        self.assertEqual(ack["payload"]["worker_id"], "WORKER-01")

        # 2. Kirim pesan tanpa format protocol standar -> Harus dibalas ERROR
        bad_msg = {"invalid_envelope": True}
        send_framed_message(client, bad_msg)

        err = recv_framed_message(client)
        self.assertEqual(err["type"], MessageType.ERROR.value)

        # Tutup koneksi
        client.close()
        server.stop()


if __name__ == "__main__":
    unittest.main()
