"""
Module: fault_tolerance.py
Fokus: Toleransi kesalahan terdistribusi, deteksi kegagalan node,
       heartbeat monitoring, task requeue, batas retry maksimal,
       dan pencegahan penghitungan ganda (idempotency).
Bagian dari: System Optimization & Performance Evaluation (Anggota 3)
"""

import time
import logging
import threading
from typing import Dict, Any, Optional, List, Callable, Set

from master.task_queue import TaskQueue, TaskStatus
from master.worker_manager import WorkerManager, WorkerInfo

logger = logging.getLogger("FaultTolerance")


class FaultToleranceManager:
    """
    Manajer Toleransi Kesalahan (Fault Tolerance) untuk Node Master.
    Bertanggung jawab:
    1. Memantau sinyal detak jantung (heartbeat) seluruh worker fisik.
    2. Mendeteksi crash/disconnect jaringan secara otomatis.
    3. Mengembalikan (requeue) task yang sedang dikerjakan worker yang gagal
       ke antrean PENDING agar dapat diselesaikan oleh worker lain yang sehat.
    4. Menerapkan batas maksimal retry (max_retries) untuk menghindari starvation.
    5. Menjaga cooldown/blacklist node yang sering mengalami kegagalan berulang.
    """

    def __init__(
        self,
        task_queue: TaskQueue,
        worker_manager: WorkerManager,
        timeout_threshold_seconds: float = 10.0,
        check_interval_seconds: float = 1.0,
        max_task_retries: int = 3,
        on_task_recovered_callback: Optional[Callable[[str, str], None]] = None,
        on_worker_failed_callback: Optional[Callable[[str], None]] = None
    ):
        self.task_queue = task_queue
        self.worker_manager = worker_manager
        self.timeout_threshold = timeout_threshold_seconds
        self.check_interval = check_interval_seconds
        self.max_task_retries = max_task_retries
        self.on_task_recovered = on_task_recovered_callback
        self.on_worker_failed = on_worker_failed_callback

        self._is_running = False
        self._monitor_thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()

        # Statistik Pemulihan Kegagalan
        self.total_recoveries: int = 0
        self.total_worker_failures: int = 0
        self.failed_worker_history: Dict[str, int] = {}
        self.blacklisted_workers: Set[str] = set()
        self.failed_tasks_exhausted: List[str] = []

    def start(self) -> None:
        """Menyalakan monitor pengawas toleransi kesalahan (background thread)."""
        with self._lock:
            if self._is_running:
                return
            self._is_running = True
            self._monitor_thread = threading.Thread(
                target=self._monitor_loop,
                name="FaultTolerance-MonitorThread",
                daemon=True
            )
            self._monitor_thread.start()
            logger.info(
                f"[FAULT TOLERANCE] Monitor aktif (Timeout: {self.timeout_threshold}s, "
                f"Interval: {self.check_interval}s, Max Retries: {self.max_task_retries})."
            )

    def stop(self) -> None:
        """Menghentikan thread monitor pengawas toleransi kesalahan."""
        with self._lock:
            self._is_running = False
            if self._monitor_thread and self._monitor_thread.is_alive():
                self._monitor_thread.join(timeout=1.5)
            self._monitor_thread = None
            logger.info("[FAULT TOLERANCE] Monitor dinonaktifkan.")

    def check_and_recover(self) -> List[str]:
        """
        Menjalankan satu siklus deteksi kegagalan dan pemulihan task.
        Mengembalikan daftar task_id yang berhasil dipulihkan (requeued).
        """
        recovered_task_ids: List[str] = []

        with self._lock:
            # 1. Periksa worker yang heartbeat-nya kedaluwarsa (timed out)
            timed_out_workers = self.worker_manager.get_timed_out_workers(self.timeout_threshold)
            for w in timed_out_workers:
                logger.warning(
                    f"[FAULT TOLERANCE] Worker {w.worker_id} tidak merespons detak jantung "
                    f"> {self.timeout_threshold:.1f}s. Menandai status DISCONNECTED."
                )
                self.worker_manager.mark_disconnected(w.worker_id)
                self._record_worker_failure(w.worker_id)

            # 2. Periksa seluruh worker yang DISCONNECTED tapi masih memegang task
            all_workers = self.worker_manager.get_all_workers()
            for w in all_workers:
                if w.connection_status == "DISCONNECTED" and w.current_task_id:
                    orphan_task_id = w.current_task_id
                    task_obj = self.task_queue.get_task(orphan_task_id)
                    task_status = self.task_queue.get_status(orphan_task_id)

                    if not task_obj or task_status == TaskStatus.COMPLETED:
                        # Task sudah selesai sebelum worker terputus, tidak perlu requeue
                        w.current_task_id = None
                        continue

                    # Periksa batas maksimal retry
                    if task_obj.retry_count >= self.max_task_retries:
                        logger.error(
                            f"[FAULT TOLERANCE] Task {orphan_task_id} telah mencapai batas "
                            f"maksimal retry ({task_obj.retry_count}/{self.max_task_retries}). "
                            f"Menghentikan retry untuk task ini."
                        )
                        if orphan_task_id not in self.failed_tasks_exhausted:
                            self.failed_tasks_exhausted.append(orphan_task_id)
                        failed_wid = w.worker_id
                        w.current_task_id = None
                        self._record_worker_failure(failed_wid)
                        continue

                    logger.warning(
                        f"[FAULT TOLERANCE] Mendeteksi task terbengkalai {orphan_task_id} "
                        f"pada worker {w.worker_id} (Retry ke-{task_obj.retry_count + 1}). "
                        f"Memulai pemulihan..."
                    )

                    success = self.task_queue.requeue_task(orphan_task_id)
                    if success:
                        self.total_recoveries += 1
                        recovered_task_ids.append(orphan_task_id)
                        failed_wid = w.worker_id
                        w.current_task_id = None
                        self._record_worker_failure(failed_wid)

                        logger.info(
                            f"[FAULT TOLERANCE] Task {orphan_task_id} berhasil dikembalikan "
                            f"ke antrean PENDING untuk ditugaskan ke worker lain."
                        )

                        if self.on_task_recovered:
                            try:
                                self.on_task_recovered(orphan_task_id, failed_wid)
                            except Exception as e:
                                logger.error(f"Error pada on_task_recovered callback: {e}")

        return recovered_task_ids

    def _record_worker_failure(self, worker_id: str) -> None:
        """Mencatat riwayat kegagalan worker untuk blacklist/cooldown jika terlalu sering bermasalah."""
        self.total_worker_failures += 1
        count = self.failed_worker_history.get(worker_id, 0) + 1
        self.failed_worker_history[worker_id] = count

        if count >= 3 and worker_id not in self.blacklisted_workers:
            self.blacklisted_workers.add(worker_id)
            logger.error(
                f"[FAULT TOLERANCE] Worker {worker_id} telah gagal sebanyak {count} kali. "
                f"Worker dimasukkan ke daftar BLACKLIST."
            )

        if self.on_worker_failed:
            try:
                self.on_worker_failed(worker_id)
            except Exception as e:
                logger.error(f"Error pada on_worker_failed callback: {e}")

    def _monitor_loop(self) -> None:
        """Loop berkala di background untuk mengawasi status kesehatan node."""
        while self._is_running:
            try:
                self.check_and_recover()
            except Exception as e:
                logger.error(f"Error pada loop monitor Fault Tolerance: {e}")
            time.sleep(self.check_interval)

    def get_summary(self) -> Dict[str, Any]:
        """Mengembalikan ringkasan statistik metrik pemulihan toleransi kesalahan."""
        with self._lock:
            return {
                "total_recoveries": self.total_recoveries,
                "total_worker_failures": self.total_worker_failures,
                "blacklisted_workers": list(self.blacklisted_workers),
                "failed_tasks_exhausted": list(self.failed_tasks_exhausted),
                "worker_failure_history": dict(self.failed_worker_history),
                "timeout_threshold_seconds": self.timeout_threshold,
                "max_task_retries": self.max_task_retries
            }
