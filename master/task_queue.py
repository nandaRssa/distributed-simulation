"""
Module: task_queue.py
Fokus: Pengelolaan antrean pekerjaan (Task Queue) berbasis Thread-Safe
       dengan pemisahan status PENDING, RUNNING, COMPLETED, dan FAILED.
Bagian dari: Master Controller (Anggota 1)
"""

import time
import threading
from collections import deque
from enum import Enum
from typing import Dict, List, Optional, Any

from .task_decomposer import Task


class TaskStatus(str, Enum):
    """Enumerasi status daur hidup (lifecycle) sebuah task."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class TaskQueue:
    """
    Antrean pekerjaan thread-safe untuk mengelola distribusi task ke beberapa Worker.
    Mendukung konkurensi multi-thread dengan locking granular, pemisahan antrean berdasarkan
    status, serta antarmuka pemulihan (requeue) untuk Fault Tolerance Anggota 3.
    """

    def __init__(self):
        self._lock = threading.RLock()
        
        # Registry seluruh task berdasarkan task_id
        self._tasks: Dict[str, Task] = {}
        
        # State tracking
        self._task_status: Dict[str, TaskStatus] = {}
        
        # Antrean FIFO untuk task yang siap dikerjakan
        self._pending_queue: deque[str] = deque()
        
        # Set / Map untuk pemantauan task aktif dan selesai
        self._running_tasks: Dict[str, Task] = {}
        self._completed_tasks: Dict[str, Task] = {}
        self._failed_tasks: Dict[str, Task] = {}

    def add_task(self, task: Task) -> None:
        """Menambahkan satu task baru ke dalam antrean (Thread-Safe)."""
        with self._lock:
            if task.task_id in self._tasks:
                raise ValueError(f"Task dengan ID {task.task_id} sudah ada dalam antrean.")
            
            self._tasks[task.task_id] = task
            self._task_status[task.task_id] = TaskStatus.PENDING
            self._pending_queue.append(task.task_id)

    def add_tasks(self, tasks: List[Task]) -> None:
        """Menambahkan sekumpulan task secara batch (Thread-Safe)."""
        with self._lock:
            for task in tasks:
                self.add_task(task)

    def get_next_task(self, worker_id: str) -> Optional[Task]:
        """
        Mengambil task berikutnya dari antrean pending dan mengubah statusnya
        menjadi RUNNING serta mencatat worker yang ditugaskan (Thread-Safe).
        Mengembalikan None jika antrean pending kosong.
        """
        with self._lock:
            if not self._pending_queue:
                return None

            task_id = self._pending_queue.popleft()
            task = self._tasks[task_id]

            # Transisi status PENDING -> RUNNING
            self._task_status[task_id] = TaskStatus.RUNNING
            task.assigned_worker_id = worker_id
            task.dispatched_at = time.time()
            self._running_tasks[task_id] = task

            return task

    def mark_completed(self, task_id: str) -> bool:
        """
        Menandai task telah berhasil diselesaikan oleh worker (Thread-Safe).
        Transisi status: RUNNING -> COMPLETED.
        Mengembalikan True jika berhasil, False jika task_id tidak dalam status RUNNING.
        """
        with self._lock:
            if task_id not in self._running_tasks:
                return False

            task = self._running_tasks.pop(task_id)
            task.completed_at = time.time()
            self._task_status[task_id] = TaskStatus.COMPLETED
            self._completed_tasks[task_id] = task
            return True

    def mark_failed(self, task_id: str, reason: str = "") -> bool:
        """
        Menandai task gagal dieksekusi (Thread-Safe).
        Transisi status: RUNNING -> FAILED.
        """
        with self._lock:
            if task_id not in self._running_tasks:
                return False

            task = self._running_tasks.pop(task_id)
            self._task_status[task_id] = TaskStatus.FAILED
            self._failed_tasks[task_id] = task
            return True

    def requeue_task(self, task_id: str) -> bool:
        """
        Antarmuka penting untuk Fault Tolerance (Anggota 3).
        Mengembalikan task dari status RUNNING atau FAILED kembali ke antrean PENDING
        agar dapat ditugaskan ke worker lain jika terjadi timeout/crash.
        """
        with self._lock:
            if task_id not in self._tasks:
                return False

            # Ambil task dari pool running atau failed
            task = None
            if task_id in self._running_tasks:
                task = self._running_tasks.pop(task_id)
            elif task_id in self._failed_tasks:
                task = self._failed_tasks.pop(task_id)
            elif self._task_status.get(task_id) == TaskStatus.PENDING:
                # Sudah pending, tidak perlu dipindahkan
                return True
            else:
                return False

            # Reset atribut penugasan dan increment retry count
            task.assigned_worker_id = None
            task.dispatched_at = None
            task.retry_count += 1

            self._task_status[task_id] = TaskStatus.PENDING
            self._pending_queue.appendleft(task_id)  # Prioritaskan eksekusi ulang
            return True

    def get_task(self, task_id: str) -> Optional[Task]:
        """Mendapatkan objek Task berdasarkan ID."""
        with self._lock:
            return self._tasks.get(task_id)

    def get_status(self, task_id: str) -> Optional[TaskStatus]:
        """Mendapatkan status saat ini dari suatu task."""
        with self._lock:
            return self._task_status.get(task_id)

    def is_all_completed(self) -> bool:
        """Memeriksa apakah seluruh task yang terdaftar telah selesai dieksekusi."""
        with self._lock:
            total_tasks = len(self._tasks)
            return total_tasks > 0 and len(self._completed_tasks) == total_tasks

    def has_pending_tasks(self) -> bool:
        """Memeriksa ketersediaan task yang belum ditugaskan."""
        with self._lock:
            return len(self._pending_queue) > 0

    def get_running_tasks(self) -> List[Task]:
        """
        Mengembalikan list task yang sedang berjalan.
        Dibutuhkan oleh Anggota 3 untuk monitoring timeout dan heartbeat worker.
        """
        with self._lock:
            return list(self._running_tasks.values())

    def get_summary(self) -> Dict[str, Any]:
        """
        Mendapatkan ringkasan statistik antrean saat ini (Thread-Safe).
        Berguna untuk monitoring dan dashboard evaluasi performa.
        """
        with self._lock:
            total = len(self._tasks)
            pending = len(self._pending_queue)
            running = len(self._running_tasks)
            completed = len(self._completed_tasks)
            failed = len(self._failed_tasks)
            progress_pct = (completed / total * 100.0) if total > 0 else 0.0

            return {
                "total": total,
                "pending": pending,
                "running": running,
                "completed": completed,
                "failed": failed,
                "progress_percent": round(progress_pct, 2)
            }
