"""
Module: worker_manager.py
Fokus: Pengelolaan siklus hidup, registrasi, metadata spek, dan pemantauan
       status koneksi serta beban kerja node Worker (10 komputer fisik).
Bagian dari: Master Controller (Anggota 1)
"""

import time
import socket as py_socket
import threading
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Any, Tuple


@dataclass
class WorkerInfo:
    """Representasi metadata dan status operasional sebuah Worker fisik."""
    worker_id: str
    ip: str
    port: int
    socket: Optional[py_socket.socket] = field(default=None, repr=False)
    connected_at: float = field(default_factory=time.time)
    last_seen: float = field(default_factory=time.time)
    connection_status: str = "CONNECTED"  # CONNECTED, DISCONNECTED
    work_status: str = "READY"            # READY, BUSY, ERROR
    cpu_cores: int = 1                    # Jumlah logical core (multiprocessing worker)
    current_task_id: Optional[str] = None
    completed_tasks_count: int = 0
    failed_tasks_count: int = 0
    total_execution_time: float = 0.0
    extra_metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Konversi informasi worker ke dictionary untuk reporting (tanpa raw socket)."""
        return {
            "worker_id": self.worker_id,
            "ip": self.ip,
            "port": self.port,
            "connected_at": self.connected_at,
            "last_seen": self.last_seen,
            "connection_status": self.connection_status,
            "work_status": self.work_status,
            "cpu_cores": self.cpu_cores,
            "current_task_id": self.current_task_id,
            "completed_tasks_count": self.completed_tasks_count,
            "failed_tasks_count": self.failed_tasks_count,
            "total_execution_time": round(self.total_execution_time, 4),
            "extra_metadata": dict(self.extra_metadata)
        }


class WorkerManager:
    """
    Manajer koneksi dan registry seluruh Worker yang terhubung ke Master.
    Thread-safe, mencatat metrik performa, dan menyediakan antarmuka
    informasi kesehatan node yang diperlukan oleh Anggota 3 (Fault Tolerance).
    """

    def __init__(self):
        self._lock = threading.RLock()
        
        # worker_id -> WorkerInfo
        self._workers: Dict[str, WorkerInfo] = {}
        
        # (ip, port) -> worker_id (pemetaan cepat dari soket ke id worker)
        self._address_to_worker_id: Dict[Tuple[str, int], str] = {}

    def register_worker(
        self,
        worker_id: str,
        ip: str,
        port: int,
        client_sock: Optional[py_socket.socket] = None,
        cpu_cores: int = 1,
        metadata: Optional[Dict[str, Any]] = None
    ) -> WorkerInfo:
        """
        Mendaftarkan worker baru atau memperbarui worker yang reconnect (Thread-Safe).
        """
        with self._lock:
            now = time.time()
            if worker_id in self._workers:
                # Reconnect
                worker = self._workers[worker_id]
                worker.ip = ip
                worker.port = port
                worker.socket = client_sock
                worker.connected_at = now
                worker.last_seen = now
                worker.connection_status = "CONNECTED"
                worker.work_status = "READY"
                worker.cpu_cores = cpu_cores
                if metadata:
                    worker.extra_metadata.update(metadata)
            else:
                worker = WorkerInfo(
                    worker_id=worker_id,
                    ip=ip,
                    port=port,
                    socket=client_sock,
                    connected_at=now,
                    last_seen=now,
                    connection_status="CONNECTED",
                    work_status="READY",
                    cpu_cores=cpu_cores,
                    extra_metadata=metadata or {}
                )
                self._workers[worker_id] = worker

            self._address_to_worker_id[(ip, port)] = worker_id
            return worker

    def mark_disconnected(self, address_or_id: Any) -> Optional[WorkerInfo]:
        """
        Menandai worker terputus tanpa menghapus histori kinerjanya (Thread-Safe).
        Menerima input berupa tuple (ip, port) atau string worker_id.
        """
        with self._lock:
            worker_id = None
            if isinstance(address_or_id, tuple):
                worker_id = self._address_to_worker_id.get(address_or_id)
            elif isinstance(address_or_id, str):
                worker_id = address_or_id

            if not worker_id or worker_id not in self._workers:
                return None

            worker = self._workers[worker_id]
            worker.connection_status = "DISCONNECTED"
            worker.work_status = "OFFLINE"
            worker.socket = None
            worker.last_seen = time.time()
            return worker

    def update_work_status(self, worker_id: str, status: str) -> bool:
        """Memperbarui status operasional worker (READY, BUSY, IDLE)."""
        with self._lock:
            if worker_id not in self._workers:
                return False
            self._workers[worker_id].work_status = status
            self._workers[worker_id].last_seen = time.time()
            return True

    def assign_task(self, worker_id: str, task_id: str) -> bool:
        """Mencatat bahwa worker sedang mengerjakan task tertentu."""
        with self._lock:
            if worker_id not in self._workers:
                return False
            worker = self._workers[worker_id]
            worker.current_task_id = task_id
            worker.work_status = "BUSY"
            worker.last_seen = time.time()
            return True

    def complete_task(
        self,
        worker_id: str,
        task_id: str,
        execution_time: float = 0.0
    ) -> bool:
        """Mencatat penyelesaian task dan mengembalikan status worker menjadi READY."""
        with self._lock:
            if worker_id not in self._workers:
                return False
            worker = self._workers[worker_id]
            if worker.current_task_id == task_id:
                worker.current_task_id = None
            worker.completed_tasks_count += 1
            worker.total_execution_time += execution_time
            worker.work_status = "READY"
            worker.last_seen = time.time()
            return True

    def fail_task(self, worker_id: str, task_id: str) -> bool:
        """Mencatat kegagalan task pada worker terkait."""
        with self._lock:
            if worker_id not in self._workers:
                return False
            worker = self._workers[worker_id]
            if worker.current_task_id == task_id:
                worker.current_task_id = None
            worker.failed_tasks_count += 1
            worker.work_status = "READY"
            worker.last_seen = time.time()
            return True

    def update_heartbeat(self, worker_id: str) -> bool:
        """
        Memperbarui timestamp aktivitas terakhir (last_seen) worker.
        Titik integrasi utama bagi modul Heartbeat Anggota 3.
        """
        with self._lock:
            if worker_id not in self._workers:
                return False
            self._workers[worker_id].last_seen = time.time()
            return True

    def get_worker(self, worker_id: str) -> Optional[WorkerInfo]:
        """Mendapatkan data worker berdasarkan ID."""
        with self._lock:
            return self._workers.get(worker_id)

    def get_worker_by_address(self, address: Tuple[str, int]) -> Optional[WorkerInfo]:
        """Mendapatkan data worker berdasarkan pasangan (ip, port)."""
        with self._lock:
            worker_id = self._address_to_worker_id.get(address)
            return self._workers.get(worker_id) if worker_id else None

    def get_all_workers(self) -> List[WorkerInfo]:
        """Mendapatkan daftar seluruh worker yang pernah terdaftar."""
        with self._lock:
            return list(self._workers.values())

    def get_connected_workers(self) -> List[WorkerInfo]:
        """Mendapatkan daftar seluruh worker yang saat ini berstatus CONNECTED."""
        with self._lock:
            return [w for w in self._workers.values() if w.connection_status == "CONNECTED"]

    def get_ready_workers(self) -> List[WorkerInfo]:
        """Mendapatkan daftar worker yang online dan siap menerima tugas (READY)."""
        with self._lock:
            return [
                w for w in self._workers.values()
                if w.connection_status == "CONNECTED" and w.work_status == "READY"
            ]

    def get_timed_out_workers(self, timeout_threshold_seconds: float) -> List[WorkerInfo]:
        """
        Mendeteksi worker yang tidak mengirim aktivitas/heartbeat melebihi batas waktu.
        Fungsi kunci untuk Anggota 3 (Fault Tolerance / Dead Node Detection).
        """
        with self._lock:
            now = time.time()
            return [
                w for w in self._workers.values()
                if w.connection_status == "CONNECTED" and (now - w.last_seen) > timeout_threshold_seconds
            ]

    def get_summary(self) -> Dict[str, Any]:
        """Mendapatkan ringkasan statistik worker terkini."""
        with self._lock:
            total = len(self._workers)
            connected = sum(1 for w in self._workers.values() if w.connection_status == "CONNECTED")
            ready = sum(1 for w in self._workers.values() if w.connection_status == "CONNECTED" and w.work_status == "READY")
            busy = sum(1 for w in self._workers.values() if w.connection_status == "CONNECTED" and w.work_status == "BUSY")
            disconnected = total - connected

            return {
                "total_registered": total,
                "connected": connected,
                "ready": ready,
                "busy": busy,
                "disconnected": disconnected
            }
