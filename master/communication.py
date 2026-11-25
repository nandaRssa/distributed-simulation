"""
Module: communication.py
Fokus: Server TCP Master berbasis Thread-per-Client, penanganan protokol JSON,
       integrasi Length-Prefixed Message Framing, dan pengelolaan koneksi multi-worker.
Bagian dari: Master Controller (Anggota 1)
"""

import socket
import threading
import logging
from typing import Dict, Any, Callable, Optional

from shared.protocol import MessageType, ProtocolMessage
from shared.message_framing import send_framed_message, recv_framed_message, FramingError

logger = logging.getLogger("MasterServer")


class MasterServer:
    """
    TCP Socket Server untuk Master Node.
    Mengelola koneksi paralel dari 9+ node Worker fisik, membaca paket framed JSON,
    mendistribusikan penanganan pesan ke callbacks, dan mengelola pemutusan koneksi.
    """

    def __init__(self, host: str = "0.0.0.0", port: int = 5000):
        self.host = host
        self.port = port
        self._server_socket: Optional[socket.socket] = None
        self._is_running = False
        self._accept_thread: Optional[threading.Thread] = None

        # Callbacks
        # msg_type -> callable(client_socket, client_address, message_dict)
        self._message_handlers: Dict[str, Callable[[socket.socket, tuple, Dict[str, Any]], None]] = {}
        self._on_connect_callback: Optional[Callable[[socket.socket, tuple], None]] = None
        self._on_disconnect_callback: Optional[Callable[[socket.socket, tuple], None]] = None

        # Lock untuk soket dan client tracking
        self._lock = threading.RLock()
        self._active_sockets: Dict[tuple, socket.socket] = {}
        self._worker_threads: Dict[tuple, threading.Thread] = {}

    def register_handler(
        self,
        msg_type: MessageType,
        handler: Callable[[socket.socket, tuple, Dict[str, Any]], None]
    ) -> None:
        """Mendaftarkan fungsi callback untuk menangani tipe pesan tertentu."""
        key = msg_type.value if isinstance(msg_type, MessageType) else str(msg_type)
        self._message_handlers[key] = handler

    def set_connection_callbacks(
        self,
        on_connect: Optional[Callable[[socket.socket, tuple], None]] = None,
        on_disconnect: Optional[Callable[[socket.socket, tuple], None]] = None
    ) -> None:
        """Mendaftarkan callback untuk event koneksi baru dan diskoneksi worker."""
        self._on_connect_callback = on_connect
        self._on_disconnect_callback = on_disconnect

    def start(self) -> None:
        """Memulai TCP Socket Server dan membuka thread penerima koneksi baru."""
        with self._lock:
            if self._is_running:
                logger.warning("Server sudah berjalan.")
                return

            self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Izinkan reuse address agar tidak terkena error TIME_WAIT saat restart
            self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            
            try:
                self._server_socket.bind((self.host, self.port))
            except socket.error as e:
                logger.error(f"Gagal melakukan bind pada {self.host}:{self.port} -> {e}")
                raise

            self._server_socket.listen(15)  # Mampu menampung backlog hingga 15 koneksi (target 9-10 node)
            self._is_running = True

            self._accept_thread = threading.Thread(
                target=self._accept_loop,
                name="Master-Accept-Loop",
                daemon=True
            )
            self._accept_thread.start()
            logger.info(f"Master TCP Server aktif dan mendengarkan pada {self.host}:{self.port}")

    def _accept_loop(self) -> None:
        """Loop thread utama untuk menerima koneksi masuk dari Worker."""
        while self._is_running:
            try:
                client_sock, client_addr = self._server_socket.accept()
                # Matikan algoritma Nagle untuk latensi transmisi minimal
                client_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

                logger.info(f"Koneksi baru diterima dari {client_addr[0]}:{client_addr[1]}")

                with self._lock:
                    self._active_sockets[client_addr] = client_sock

                # Panggil callback on_connect jika ada
                if self._on_connect_callback:
                    try:
                        self._on_connect_callback(client_sock, client_addr)
                    except Exception as e:
                        logger.error(f"Error pada on_connect_callback untuk {client_addr}: {e}")

                # Buat thread penangan khusus untuk worker ini
                handler_thread = threading.Thread(
                    target=self._client_handler_loop,
                    args=(client_sock, client_addr),
                    name=f"Handler-{client_addr[0]}:{client_addr[1]}",
                    daemon=True
                )
                with self._lock:
                    self._worker_threads[client_addr] = handler_thread
                handler_thread.start()

            except socket.error:
                if not self._is_running:
                    break
                logger.warning("Socket error saat accept koneksi baru.")

    def _client_handler_loop(self, client_sock: socket.socket, client_addr: tuple) -> None:
        """Thread loop yang bertugas membaca pesan dari satu client tertentu."""
        try:
            while self._is_running:
                message = recv_framed_message(client_sock)
                if message is None:
                    # Client terputus secara normal (EOF)
                    logger.info(f"Worker di {client_addr} menutup koneksi secara normal.")
                    break

                if not ProtocolMessage.validate(message):
                    logger.warning(f"Menerima pesan dengan format tidak valid dari {client_addr}: {message}")
                    error_reply = ProtocolMessage.create(
                        MessageType.ERROR,
                        {"error": "Format pesan tidak valid. Wajib memiliki field 'type' dan 'payload'."}
                    )
                    self.send_message(client_sock, error_reply)
                    continue

                msg_type = message.get("type")
                handler = self._message_handlers.get(msg_type)

                if handler:
                    try:
                        handler(client_sock, client_addr, message)
                    except Exception as e:
                        logger.exception(f"Error saat mengeksekusi handler untuk pesan '{msg_type}' dari {client_addr}: {e}")
                        error_reply = ProtocolMessage.create(
                            MessageType.ERROR,
                            {"error": f"Internal Server Error saat memproses pesan {msg_type}: {str(e)}"}
                        )
                        self.send_message(client_sock, error_reply)
                else:
                    logger.warning(f"Tipe pesan tidak dikenal '{msg_type}' dari {client_addr}")
                    err = ProtocolMessage.create(
                        MessageType.ERROR,
                        {"error": f"Handler untuk tipe pesan '{msg_type}' tidak ditemukan."}
                    )
                    self.send_message(client_sock, err)

        except (socket.error, ConnectionResetError, BrokenPipeError, FramingError) as e:
            logger.warning(f"Koneksi terputus tak terduga dengan {client_addr}: {e}")
        finally:
            self._cleanup_client(client_sock, client_addr)

    def _cleanup_client(self, client_sock: socket.socket, client_addr: tuple) -> None:
        """Membersihkan soket client dan memicu callback disconnect."""
        with self._lock:
            if client_addr in self._active_sockets:
                del self._active_sockets[client_addr]
            if client_addr in self._worker_threads:
                del self._worker_threads[client_addr]

        try:
            client_sock.close()
        except Exception:
            pass

        if self._on_disconnect_callback:
            try:
                self._on_disconnect_callback(client_sock, client_addr)
            except Exception as e:
                logger.error(f"Error pada on_disconnect_callback untuk {client_addr}: {e}")

    def send_message(self, client_sock: socket.socket, message: Dict[str, Any]) -> bool:
        """Mengirim pesan framed JSON ke soket client target (Thread-Safe)."""
        try:
            send_framed_message(client_sock, message)
            return True
        except Exception as e:
            logger.error(f"Gagal mengirim pesan ke socket: {e}")
            return False

    def broadcast(self, message: Dict[str, Any]) -> None:
        """Mengirim pesan broadcast framed JSON ke seluruh worker yang aktif."""
        with self._lock:
            sockets = list(self._active_sockets.values())

        for sock in sockets:
            self.send_message(sock, message)

    def stop(self) -> None:
        """Menghentikan Master TCP Server secara teratur (Graceful Shutdown)."""
        logger.info("Menghentikan Master Server...")
        with self._lock:
            self._is_running = False

            # Kirim pesan STOP ke seluruh worker aktif
            stop_msg = ProtocolMessage.create(
                MessageType.STOP,
                {"reason": "Master server shutting down."}
            )
            for sock in list(self._active_sockets.values()):
                try:
                    self.send_message(sock, stop_msg)
                    sock.close()
                except Exception:
                    pass

            self._active_sockets.clear()

            # Tutup server listener socket
            if self._server_socket:
                try:
                    self._server_socket.close()
                except Exception:
                    pass
                self._server_socket = None

        logger.info("Master Server telah berhenti.")
