"""
Unit Test: test_task_queue.py
Menguji fungsionalitas TaskQueue, Finite State Machine (PENDING, RUNNING, COMPLETED, FAILED),
mekanisme Requeue (Fault Tolerance), serta Thread-Safety terhadap akses konkuren simultan.
"""

import sys
import os
import unittest
import threading
from typing import List

# Menambahkan direktori project root ke sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from master.task_decomposer import Task
from master.task_queue import TaskQueue, TaskStatus


class TestTaskQueue(unittest.TestCase):

    def setUp(self):
        self.queue = TaskQueue()
        self.sample_config = {"population": 1000, "beta": 0.3}

    def _create_task(self, task_id: str, iters: int = 100) -> Task:
        return Task(
            task_id=task_id,
            iterations=iters,
            simulation_config=self.sample_config,
            random_seed=12345
        )

    def test_add_and_get_task(self):
        """Uji penambahan dan pengambilan task dalam urutan FIFO."""
        t1 = self._create_task("T-1")
        t2 = self._create_task("T-2")
        self.queue.add_tasks([t1, t2])

        self.assertEqual(self.queue.get_status("T-1"), TaskStatus.PENDING)
        self.assertEqual(self.queue.get_status("T-2"), TaskStatus.PENDING)

        # Ambil task pertama oleh worker-01
        popped_1 = self.queue.get_next_task(worker_id="worker-01")
        self.assertIsNotNone(popped_1)
        self.assertEqual(popped_1.task_id, "T-1")
        self.assertEqual(popped_1.assigned_worker_id, "worker-01")
        self.assertEqual(self.queue.get_status("T-1"), TaskStatus.RUNNING)

        # Ambil task kedua oleh worker-02
        popped_2 = self.queue.get_next_task(worker_id="worker-02")
        self.assertIsNotNone(popped_2)
        self.assertEqual(popped_2.task_id, "T-2")
        self.assertEqual(self.queue.get_status("T-2"), TaskStatus.RUNNING)

        # Antrean sekarang harus kosong
        self.assertIsNone(self.queue.get_next_task(worker_id="worker-03"))

    def test_lifecycle_complete(self):
        """Uji daur hidup penuh dari PENDING -> RUNNING -> COMPLETED."""
        t = self._create_task("T-100")
        self.queue.add_task(t)

        self.assertFalse(self.queue.is_all_completed())
        
        # Dispatch
        dispatched = self.queue.get_next_task(worker_id="worker-A")
        self.assertEqual(self.queue.get_status("T-100"), TaskStatus.RUNNING)

        # Selesaikan
        success = self.queue.mark_completed("T-100")
        self.assertTrue(success)
        self.assertEqual(self.queue.get_status("T-100"), TaskStatus.COMPLETED)
        self.assertTrue(self.queue.is_all_completed())

        summary = self.queue.get_summary()
        self.assertEqual(summary["completed"], 1)
        self.assertEqual(summary["pending"], 0)
        self.assertEqual(summary["progress_percent"], 100.0)

    def test_requeue_failed_or_timed_out_task(self):
        """Uji mekanisme requeue untuk mendukung Fault Tolerance milik Anggota 3."""
        t = self._create_task("T-RETRY")
        self.queue.add_task(t)

        # Worker mengambil task
        self.queue.get_next_task("worker-fail")
        self.assertEqual(self.queue.get_status("T-RETRY"), TaskStatus.RUNNING)

        # Simulasi worker crash / timeout -> Requeue task
        requeued = self.queue.requeue_task("T-RETRY")
        self.assertTrue(requeued)
        self.assertEqual(self.queue.get_status("T-RETRY"), TaskStatus.PENDING)

        # Ambil kembali oleh worker baru yang sehat
        retry_task = self.queue.get_next_task("worker-healthy")
        self.assertIsNotNone(retry_task)
        self.assertEqual(retry_task.task_id, "T-RETRY")
        self.assertEqual(retry_task.assigned_worker_id, "worker-healthy")
        self.assertEqual(retry_task.retry_count, 1)

    def test_duplicate_task_id_rejection(self):
        """Pastikan task_id yang sama ditolak untuk mencegah overwrite."""
        t1 = self._create_task("T-DUP")
        t2 = self._create_task("T-DUP")
        self.queue.add_task(t1)

        with self.assertRaises(ValueError):
            self.queue.add_task(t2)

    def test_thread_safety_concurrent_access(self):
        """
        Stress test thread-safety: 10 worker threads bersaing mengambil 100 task secara simultan.
        Setiap task harus diambil tepat satu kali, tidak boleh ada race condition / double-assignment.
        """
        TOTAL_TASKS = 100
        NUM_WORKERS = 10

        tasks = [self._create_task(f"CONCURRENT-{i}") for i in range(TOTAL_TASKS)]
        self.queue.add_tasks(tasks)

        assigned_tasks_per_worker: Dict[str, List[str]] = {f"W-{i}": [] for i in range(NUM_WORKERS)}
        lock = threading.Lock()

        def worker_routine(worker_id: str):
            while True:
                task = self.queue.get_next_task(worker_id)
                if not task:
                    break
                with lock:
                    assigned_tasks_per_worker[worker_id].append(task.task_id)
                # Tandai selesai segera
                self.queue.mark_completed(task.task_id)

        threads = []
        for i in range(NUM_WORKERS):
            th = threading.Thread(target=worker_routine, args=(f"W-{i}",))
            threads.append(th)
            th.start()

        for th in threads:
            th.join()

        # Validasi
        all_assigned = []
        for w_tasks in assigned_tasks_per_worker.values():
            all_assigned.extend(w_tasks)

        self.assertEqual(len(all_assigned), TOTAL_TASKS, "Jumlah task yang diambil tidak sesuai total!")
        self.assertEqual(len(set(all_assigned)), TOTAL_TASKS, "Terjadi duplikasi penugasan antar thread!")
        self.assertTrue(self.queue.is_all_completed(), "Seluruh task harus berstatus COMPLETED!")


if __name__ == "__main__":
    unittest.main()
