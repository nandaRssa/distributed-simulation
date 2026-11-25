"""
Unit Test: test_task_scheduler.py
Menguji fungsionalitas TaskScheduler, strategi penjadwalan FIFO,
pencatatan durasi & histori penugasan, penghitungan metrik performa worker,
serta pembuktian integrasi Dynamic Load Balancing Strategy (Anggota 3).
"""

import sys
import os
import unittest
import time
from typing import Optional, Dict, Any

# Menambahkan direktori project root ke sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from master.task_decomposer import Task
from master.task_queue import TaskQueue, TaskStatus
from master.task_scheduler import TaskScheduler, BaseSchedulingStrategy, FIFOSchedulingStrategy


class TestTaskScheduler(unittest.TestCase):

    def setUp(self):
        self.queue = TaskQueue()
        self.scheduler = TaskScheduler(task_queue=self.queue)
        
        # Inisialisasi beberapa task
        for i in range(1, 4):
            self.queue.add_task(
                Task(
                    task_id=f"TASK-{i:03d}",
                    iterations=1000,
                    simulation_config={},
                    random_seed=i * 100
                )
            )

    def test_fifo_dispatch(self):
        """Uji alokasi task berurutan sesuai FIFO."""
        t1 = self.scheduler.dispatch_next_task(worker_id="WORKER-1")
        self.assertIsNotNone(t1)
        self.assertEqual(t1.task_id, "TASK-001")
        self.assertEqual(t1.assigned_worker_id, "WORKER-1")

        t2 = self.scheduler.dispatch_next_task(worker_id="WORKER-2")
        self.assertIsNotNone(t2)
        self.assertEqual(t2.task_id, "TASK-002")

    def test_record_completion_and_metrics(self):
        """Uji pencatatan task selesai dan kalkulasi metrik durasi."""
        task = self.scheduler.dispatch_next_task(worker_id="WORKER-FAST")
        time.sleep(0.01)  # Simulasi proses berjalan

        success = self.scheduler.record_task_completion(
            task_id=task.task_id,
            worker_id="WORKER-FAST",
            worker_execution_time=1.25  # Worker melaporkan eksekusi 1.25s
        )
        self.assertTrue(success)
        self.assertEqual(self.queue.get_status(task.task_id), TaskStatus.COMPLETED)

        # Periksa statistik performa worker
        stats = self.scheduler.get_worker_performance_stats()
        self.assertIn("WORKER-FAST", stats)
        self.assertEqual(stats["WORKER-FAST"]["tasks_completed"], 1)
        self.assertEqual(stats["WORKER-FAST"]["total_duration"], 1.25)
        self.assertEqual(stats["WORKER-FAST"]["average_duration"], 1.25)

    def test_record_failure(self):
        """Uji pencatatan kegagalan penugasan."""
        task = self.scheduler.dispatch_next_task(worker_id="WORKER-CRASH")
        success = self.scheduler.record_task_failure(
            task_id=task.task_id,
            worker_id="WORKER-CRASH",
            reason="Worker timeout"
        )
        self.assertTrue(success)
        self.assertEqual(self.queue.get_status(task.task_id), TaskStatus.FAILED)

        stats = self.scheduler.get_worker_performance_stats()
        self.assertEqual(stats["WORKER-CRASH"]["tasks_failed"], 1)

    def test_dynamic_load_balancing_strategy_injection(self):
        """
        Membuktikan antarmuka Anggota 3:
        Menyuntikkan custom strategy (Dynamic Load Balancing) tanpa merombak scheduler.
        """
        class MockLoadBalancingStrategy(BaseSchedulingStrategy):
            """Strategi simulasi milik Anggota 3: memprioritaskan worker dengan core terbanyak."""
            def select_task(
                self,
                task_queue: TaskQueue,
                worker_id: str,
                worker_info: Optional[Dict[str, Any]] = None
            ) -> Optional[Task]:
                # Cek spek worker
                cores = (worker_info or {}).get("cores", 1)
                # Jika worker memiliki banyak core (>= 4), berikan task
                if cores >= 4:
                    return task_queue.get_next_task(worker_id)
                return None

        # Ganti strategi default dengan dynamic strategy
        self.scheduler.set_strategy(MockLoadBalancingStrategy())

        # Worker lemah (2 core) ditolak mendapat task
        weak_worker_task = self.scheduler.dispatch_next_task(
            worker_id="WORKER-WEAK",
            worker_info={"cores": 2}
        )
        self.assertIsNone(weak_worker_task)

        # Worker kuat (8 core) berhasil mendapat task
        strong_worker_task = self.scheduler.dispatch_next_task(
            worker_id="WORKER-STRONG",
            worker_info={"cores": 8}
        )
        self.assertIsNotNone(strong_worker_task)
        self.assertEqual(strong_worker_task.task_id, "TASK-001")


if __name__ == "__main__":
    unittest.main()
