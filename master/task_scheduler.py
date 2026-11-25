"""
Module: task_scheduler.py
Fokus: Penjadwalan dan orkestrasi distribusi tugas dari TaskQueue ke Worker.
       Menerapkan Strategy Pattern agar Dynamic Load Balancing (Anggota 3)
       dapat diinjeksi tanpa merombak arsitektur komunikasi Master.
Bagian dari: Master Controller (Anggota 1)
"""

import time
import threading
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any

from .task_decomposer import Task
from .task_queue import TaskQueue, TaskStatus


class BaseSchedulingStrategy(ABC):
    """
    Antarmuka abstrak untuk algoritma penjadwalan.
    Anggota 3 dapat mengimplementasikan kelas turunan dari antarmuka ini
    untuk algoritma Dynamic Load Balancing (misal: Work Stealing atau Weighted Chunking).
    """

    @abstractmethod
    def select_task(
        self,
        task_queue: TaskQueue,
        worker_id: str,
        worker_info: Optional[Dict[str, Any]] = None
    ) -> Optional[Task]:
        """
        Memilih task berikutnya untuk worker_id tertentu dari task_queue.
        Mengembalikan Task jika tersedia, atau None jika tidak ada task yang cocok.
        """
        pass


class FIFOSchedulingStrategy(BaseSchedulingStrategy):
    """
    Strategi penjadwalan default (Anggota 1):
    First-In First-Out (FIFO) Pull-Based Scheduling.
    Worker yang meminta task akan langsung diberikan task terdepan dari antrean pending.
    """

    def select_task(
        self,
        task_queue: TaskQueue,
        worker_id: str,
        worker_info: Optional[Dict[str, Any]] = None
    ) -> Optional[Task]:
        return task_queue.get_next_task(worker_id)


class TaskScheduler:
    """
    Orkestrator penjadwalan tugas. Menghubungkan antrean task dengan worker,
    mencatat histori penugasan, memantau durasi eksekusi, serta menyediakan
    titik integrasi fleksibel untuk Dynamic Load Balancing.
    """

    def __init__(self, task_queue: TaskQueue, strategy: Optional[BaseSchedulingStrategy] = None):
        self._lock = threading.RLock()
        self.task_queue = task_queue
        self.strategy: BaseSchedulingStrategy = strategy or FIFOSchedulingStrategy()
        
        # Histori penugasan untuk evaluasi kinerja & statistik
        # task_id -> {worker_id, dispatched_at, completed_at, duration, status}
        self._dispatch_history: Dict[str, Dict[str, Any]] = {}

    def set_strategy(self, strategy: BaseSchedulingStrategy) -> None:
        """
        Mengubah strategi penjadwalan secara dinamis saat runtime.
        Titik integrasi utama bagi Anggota 3 (Dynamic Load Balancing).
        """
        with self._lock:
            if not isinstance(strategy, BaseSchedulingStrategy):
                raise TypeError("Strategi harus merupakan turunan dari BaseSchedulingStrategy.")
            self.strategy = strategy

    def dispatch_next_task(
        self,
        worker_id: str,
        worker_info: Optional[Dict[str, Any]] = None
    ) -> Optional[Task]:
        """
        Memilih dan mendistribusikan task berikutnya untuk Worker yang tersedia (Thread-Safe).
        Mencatat waktu pengiriman dan asosiasi worker.
        """
        with self._lock:
            task = self.strategy.select_task(self.task_queue, worker_id, worker_info)
            if task is not None:
                self._dispatch_history[task.task_id] = {
                    "task_id": task.task_id,
                    "worker_id": worker_id,
                    "dispatched_at": task.dispatched_at or time.time(),
                    "completed_at": None,
                    "duration": None,
                    "status": TaskStatus.RUNNING.value
                }
            return task

    def record_task_completion(
        self,
        task_id: str,
        worker_id: str,
        worker_execution_time: Optional[float] = None
    ) -> bool:
        """
        Mencatat penyelesaian task, memperbarui status pada TaskQueue,
        dan menghitung metrik durasi waktu eksekusi (turnaround time).
        """
        with self._lock:
            success = self.task_queue.mark_completed(task_id)
            if not success:
                return False

            now = time.time()
            if task_id in self._dispatch_history:
                record = self._dispatch_history[task_id]
                record["completed_at"] = now
                record["status"] = TaskStatus.COMPLETED.value
                
                # Gunakan execution time dari worker jika dilaporkan, atau hitung selisih waktu master
                if worker_execution_time is not None:
                    record["duration"] = worker_execution_time
                elif record.get("dispatched_at"):
                    record["duration"] = now - record["dispatched_at"]

            return True

    def record_task_failure(
        self,
        task_id: str,
        worker_id: str,
        reason: str = ""
    ) -> bool:
        """
        Mencatat kegagalan task pada TaskQueue dan histori.
        """
        with self._lock:
            success = self.task_queue.mark_failed(task_id, reason)
            if task_id in self._dispatch_history:
                record = self._dispatch_history[task_id]
                record["completed_at"] = time.time()
                record["status"] = TaskStatus.FAILED.value
                record["error_reason"] = reason
            return success

    def get_dispatch_history(self) -> List[Dict[str, Any]]:
        """Mendapatkan salinan histori penugasan task."""
        with self._lock:
            return list(self._dispatch_history.values())

    def get_worker_performance_stats(self) -> Dict[str, Dict[str, Any]]:
        """
        Menghasilkan statistik performa per worker (jumlah task selesai, rata-rata durasi).
        Sangat berguna untuk feed data algoritma Dynamic Load Balancing Anggota 3.
        """
        with self._lock:
            stats: Dict[str, Dict[str, Any]] = {}
            for record in self._dispatch_history.values():
                w_id = record["worker_id"]
                if w_id not in stats:
                    stats[w_id] = {
                        "tasks_assigned": 0,
                        "tasks_completed": 0,
                        "tasks_failed": 0,
                        "total_duration": 0.0,
                        "average_duration": 0.0
                    }

                stats[w_id]["tasks_assigned"] += 1
                if record["status"] == TaskStatus.COMPLETED.value:
                    stats[w_id]["tasks_completed"] += 1
                    dur = record.get("duration") or 0.0
                    stats[w_id]["total_duration"] += dur
                elif record["status"] == TaskStatus.FAILED.value:
                    stats[w_id]["tasks_failed"] += 1

            # Hitung rata-rata
            for w_id, data in stats.items():
                completed = data["tasks_completed"]
                if completed > 0:
                    data["average_duration"] = round(data["total_duration"] / completed, 4)
                    data["total_duration"] = round(data["total_duration"], 4)

            return stats
