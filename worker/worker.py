"""
Module: worker.py
Fokus: Program Client Worker (Referensi Integrasi Kontrak Anggota 2).
       Menghubungkan diri ke Master, menerima penugasan TASK, mengeksekusi
       Monte Carlo SIR (stochastic simulation), dan mengirimkan RESULT kembali ke Master.
Bagian dari: Worker Node (Digunakan oleh Anggota 2 dan untuk Pengujian Integrasi)
"""

import sys
import os
import time
import socket
import logging
import argparse
import threading
import multiprocessing
from typing import Dict, Any, Optional

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from shared.protocol import MessageType, ProtocolMessage
from shared.message_framing import send_framed_message, recv_framed_message
from worker.simulation_engine import execute_task, initialize_engine, shutdown_engine

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("WorkerClient")


class WorkerNode:
    """Implementasi klien Worker sesuai kontrak komunikasi terdistribusi."""

    def __init__(self, master_host: str, master_port: int, worker_id: str, cores: int = 4, processes: Optional[int] = None):
        self.master_host = master_host
        self.master_port = master_port
        self.worker_id = worker_id
        self.cores = cores
        self.processes = int(processes or (os.cpu_count() or 1))
        self.sock: Optional[socket.socket] = None
        self.is_running = False
        self._send_lock = threading.Lock()
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._heartbeat_interval_seconds = 2.5

    def _send_message(self, message: Dict[str, Any]) -> None:
        """Kirim pesan ke socket worker secara thread-safe."""
        with self._send_lock:
            if self.sock is None:
                raise ConnectionError("Socket worker tidak tersedia.")
            send_framed_message(self.sock, message)

    def _start_heartbeat_thread(self) -> None:
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            return
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"Heartbeat-{self.worker_id}",
            daemon=True
        )
        self._heartbeat_thread.start()

    def _heartbeat_loop(self) -> None:
        while self.is_running and not self._stop_event.is_set():
            heartbeat_msg = ProtocolMessage.create(
                MessageType.HEARTBEAT,
                {"worker_id": self.worker_id, "timestamp": time.time()}
            )
            try:
                self._send_message(heartbeat_msg)
            except Exception as e:
                if self.is_running:
                    logger.warning(f"Gagal mengirim heartbeat: {e}")
                break

            # Gunakan wait agar thread heartbeat bisa berhenti cepat saat stop.
            if self._stop_event.wait(timeout=self._heartbeat_interval_seconds):
                break

    def connect(self) -> bool:
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.sock.connect((self.master_host, self.master_port))
            logger.info(f"Terhubung ke Master pada {self.master_host}:{self.master_port}")
            return True
        except Exception as e:
            logger.error(f"Gagal terhubung ke Master: {e}")
            return False

    def start(self) -> None:
        if not self.connect():
            return

        self.is_running = True
        self._stop_event.clear()
        try:
            initialize_engine(self.processes)

            # 1. Kirim REGISTER
            reg_msg = ProtocolMessage.create(
                MessageType.REGISTER,
                {
                    "worker_id": self.worker_id,
                    "cores": self.cores,
                    "metadata": {"os": sys.platform}
                }
            )
            self._send_message(reg_msg)

            # 2. Terima REGISTER_ACK
            ack = recv_framed_message(self.sock)
            logger.info(f"Registrasi diterima oleh Master: {ack}")

            # 3. Kirim STATUS READY
            status_msg = ProtocolMessage.create(MessageType.STATUS, {"status": "READY"})
            self._send_message(status_msg)

            # 4. Jalankan heartbeat periodik worker (tetap aktif saat komputasi task).
            self._start_heartbeat_thread()

            # 5. Loop penanganan pesan
            while self.is_running:
                msg = recv_framed_message(self.sock)
                if msg is None:
                    logger.info("Koneksi ditutup oleh Master.")
                    break

                msg_type = msg.get("type")
                payload = msg.get("payload", {})

                if msg_type == MessageType.TASK.value:
                    self._handle_task(payload)
                elif msg_type == MessageType.STOP.value:
                    logger.info(f"Menerima sinyal STOP dari Master: {payload.get('reason')}")
                    break
                elif msg_type == MessageType.RESULT_ACK.value:
                    logger.info(f"Hasil {payload.get('task_id')} berhasil diterima Master.")
                    # Minta task baru dengan mengirim status READY
                    ready_msg = ProtocolMessage.create(MessageType.STATUS, {"status": "READY"})
                    self._send_message(ready_msg)
                elif msg_type == MessageType.HEARTBEAT.value:
                    ack = ProtocolMessage.create(MessageType.HEARTBEAT_ACK, {"status": "ALIVE"})
                    self._send_message(ack)
                elif msg_type == MessageType.HEARTBEAT_ACK.value:
                    # ACK heartbeat dari Master normal terjadi, tidak perlu warning.
                    continue
                else:
                    logger.warning(f"Tipe pesan tidak diantisipasi: {msg_type}")

        except Exception as e:
            logger.error(f"Error pada loop worker: {e}")
        finally:
            self.stop()

    def _handle_task(self, payload: Dict[str, Any]) -> None:
        task_id = payload.get("task_id")
        iterations = payload.get("iterations", 1000)

        logger.info(f"Mengeksekusi {task_id}: {iterations} iterasi...")
        task_payload = dict(payload)
        task_payload["worker_id"] = self.worker_id
        result_payload = execute_task(task_payload, self.processes)

        logger.info(
            f"Task {task_id} selesai dalam {result_payload['execution_time_seconds']:.3f}s. Mengirim hasil..."
        )

        result_msg = ProtocolMessage.create(MessageType.RESULT, result_payload)
        self._send_message(result_msg)

    def stop(self) -> None:
        self.is_running = False
        self._stop_event.set()

        if (
            self._heartbeat_thread
            and self._heartbeat_thread.is_alive()
            and threading.current_thread() is not self._heartbeat_thread
        ):
            self._heartbeat_thread.join(timeout=1.0)
        self._heartbeat_thread = None

        if self.sock:
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None

        shutdown_engine()
        logger.info(f"Worker {self.worker_id} berhenti.")


def parse_args():
    parser = argparse.ArgumentParser(description="Worker Client - Distributed Simulation")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Master Host IP")
    parser.add_argument("--port", type=int, default=5000, help="Master Port")
    parser.add_argument("--id", type=str, default=f"WORKER-{socket.gethostname()}", help="Worker ID")
    parser.add_argument("--cores", type=int, default=os.cpu_count() or 4, help="Jumlah core CPU")
    parser.add_argument("--processes", type=int, default=os.cpu_count() or 1, help="Jumlah proses multiprocessing")
    return parser.parse_args()


if __name__ == "__main__":
    multiprocessing.freeze_support()
    args = parse_args()
    worker = WorkerNode(
        master_host=args.host,
        master_port=args.port,
        worker_id=args.id,
        cores=args.cores,
        processes=args.processes
    )
    worker.start()
