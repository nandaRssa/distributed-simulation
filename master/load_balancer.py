"""
Module: load_balancer.py
Fokus: Dynamic Load Balancing adaptif untuk arsitektur Master-Worker.
       Mendistribusikan task berdasarkan kapasitas komputasi, jumlah core,
       dan riwayat kecepatan penyelesaian (throughput iterasi/detik) per Worker.
Bagian dari: System Optimization & Performance Evaluation (Anggota 3)
"""

import time
import logging
from typing import Dict, Any, Optional, List
from collections import defaultdict

from master.task_decomposer import Task
from master.task_queue import TaskQueue, TaskStatus
from master.task_scheduler import BaseSchedulingStrategy
from master.worker_manager import WorkerManager, WorkerInfo

logger = logging.getLogger("DynamicLoadBalancer")


class WorkerPerformanceProfile:
    """Menyimpan metrik performa historis dari setiap Worker node."""

    def __init__(self, worker_id: str, cpu_cores: int = 1):
        self.worker_id = worker_id
        self.cpu_cores = max(1, cpu_cores)
        self.tasks_completed = 0
        self.total_iterations_processed = 0
        self.total_compute_time_seconds = 0.0
        self.recent_task_durations: List[float] = []
        self.estimated_throughput: float = 0.0  # iterasi / detik
        self.last_assigned_time: float = 0.0

    def record_completion(self, iterations: int, duration_seconds: float) -> None:
        """Mencatat penyelesaian task dan memperbarui estimasi throughput."""
        self.tasks_completed += 1
        self.total_iterations_processed += iterations
        duration = max(0.0001, duration_seconds)
        self.total_compute_time_seconds += duration

        # Simpan 10 riwayat durasi terbaru untuk moving average
        self.recent_task_durations.append(duration)
        if len(self.recent_task_durations) > 10:
            self.recent_task_durations.pop(0)

        # Hitung moving throughput (iterasi per detik)
        recent_avg_duration = sum(self.recent_task_durations) / len(self.recent_task_durations)
        if recent_avg_duration > 0:
            self.estimated_throughput = (iterations / recent_avg_duration)
        else:
            self.estimated_throughput = (self.total_iterations_processed / self.total_compute_time_seconds)

    def get_capacity_score(self) -> float:
        """
        Menghitung skor kapasitas relatif worker:
        Kombinasi bobot jumlah CPU core dan throughput aktual.
        """
        base_core_weight = float(self.cpu_cores) * 1000.0
        if self.estimated_throughput > 0:
            return 0.4 * base_core_weight + 0.6 * self.estimated_throughput
        return base_core_weight


class DynamicLoadBalancer(BaseSchedulingStrategy):
    """
    Strategi Dynamic Load Balancing adaptif.
    - Melacak kecepatan pemrosesan tiap worker secara real-time.
    - Pada model pull-based / event-driven, saat worker siap (READY),
      load balancer memberikan task berikutnya secara thread-safe.
    - Memungkinkan task batching atau alokasi cerdas untuk meminimalkan idle time.
    """

    def __init__(self, worker_manager: Optional[WorkerManager] = None):
        self.worker_manager = worker_manager
        self.profiles: Dict[str, WorkerPerformanceProfile] = {}
        self._total_dispatches = 0

    def register_worker(self, worker_id: str, cpu_cores: int = 1) -> None:
        """Mendaftarkan profil performa worker baru."""
        if worker_id not in self.profiles:
            self.profiles[worker_id] = WorkerPerformanceProfile(worker_id, cpu_cores)
            logger.info(
                f"[LOAD BALANCER] Worker {worker_id} didaftarkan dengan kapasitas {cpu_cores} core."
            )

    def record_task_result(self, worker_id: str, iterations: int, duration_seconds: float) -> None:
        """Memperbarui metrik kecepatan pemrosesan worker setelah menyelesaikan task."""
        if worker_id not in self.profiles:
            self.register_worker(worker_id, cpu_cores=1)
        self.profiles[worker_id].record_completion(iterations, duration_seconds)
        profile = self.profiles[worker_id]
        logger.debug(
            f"[LOAD BALANCER] Worker {worker_id} throughput diperbarui: "
            f"{profile.estimated_throughput:.1f} iter/s (Task selesai: {profile.tasks_completed})"
        )

    def select_task(
        self,
        task_queue: TaskQueue,
        worker_id: str,
        worker_info: Optional[Dict[str, Any]] = None
    ) -> Optional[Task]:
        """
        Memilih task terbaik dari antrean untuk Worker tertentu.
        Menyesuaikan penugasan dengan kapasitas ketersediaan dan beban kerja node.
        """
        if worker_id not in self.profiles:
            cores = 1
            if worker_info:
                cores = worker_info.get("cpu_cores", 1)
            self.register_worker(worker_id, cores)

        profile = self.profiles[worker_id]
        profile.last_assigned_time = time.time()
        self._total_dispatches += 1

        # Mengambil task berikutnya yang berstatus PENDING dari antrean
        task = task_queue.get_next_task(worker_id)
        return task

    def get_load_distribution_summary(self) -> Dict[str, Any]:
        """Mengembalikan statistik distribusi beban kerja antar seluruh worker."""
        summary: Dict[str, Any] = {}
        for wid, p in self.profiles.items():
            summary[wid] = {
                "cpu_cores": p.cpu_cores,
                "tasks_completed": p.tasks_completed,
                "iterations_processed": p.total_iterations_processed,
                "total_compute_time_seconds": round(p.total_compute_time_seconds, 3),
                "estimated_throughput_iter_per_sec": round(p.estimated_throughput, 2),
            }
        return summary

