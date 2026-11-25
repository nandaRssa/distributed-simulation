"""
Integration Test: test_optimization_integration.py
Menguji integrasi optimasi:
1. Dynamic Load Balancing Strategy Hook (Anggota 3).
2. Fault Tolerance & Task Recovery: Simulasi worker crash mendadak di tengah jalan,
   pemulihan task dari status RUNNING ke PENDING, dan penyelesaian ulang oleh worker lain.
"""

import sys
import os
import unittest
import socket
import threading
import time
import tempfile
import json
from unittest.mock import patch

# Menambahkan direktori project root ke sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from shared.protocol import MessageType, ProtocolMessage
from shared.message_framing import send_framed_message, recv_framed_message
from master.master import MasterController
from master.optimization_hooks import FaultToleranceManager, DynamicLoadBalancingStrategy
from master.task_queue import TaskQueue, TaskStatus
from master.worker_manager import WorkerManager
from master.task_decomposer import Task
from worker.worker import WorkerNode


class TestOptimizationIntegration(unittest.TestCase):

    def setUp(self):
        # Cari port bebas
        temp_s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        temp_s.bind(("127.0.0.1", 0))
        self.port = temp_s.getsockname()[1]
        temp_s.close()

        self.temp_dir = tempfile.TemporaryDirectory()
        self.output_json = os.path.join(self.temp_dir.name, "opt_result.json")

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_fault_tolerance_requeue_on_crash(self):
        """
        Pengujian Fault Tolerance:
        1. 2 task (@ 500 iterasi).
        2. Worker 1 mengambil TASK-0001 lalu CRASH (soket ditutup paksa sebelum mengirim RESULT).
        3. Master mendeteksi pemutusan koneksi Worker 1 dan me-requeue TASK-0001.
        4. Worker 2 (sehat) mengambil TASK-0001 yang dipulihkan dan menyelesaikannya.
        5. Seluruh simulasi selesai 100% tanpa ada task yang hilang!
        """
        TOTAL_ITERS = 1000
        CHUNK_SIZE = 500

        master = MasterController(
            host="127.0.0.1",
            port=self.port,
            total_iterations=TOTAL_ITERS,
            chunk_size=CHUNK_SIZE,
            min_workers=1,
            output_file=self.output_json,
            enable_fault_tolerance=True,
            worker_timeout_seconds=8.0
        )

        master_thread = threading.Thread(target=master.start, args=(15.0,), name="Master-FaultTolerance")
        master_thread.start()
        time.sleep(0.1)

        # Worker 1: Worker nakal / akan crash
        s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s1.connect(("127.0.0.1", self.port))

        # Register Worker 1
        send_framed_message(s1, ProtocolMessage.create(MessageType.REGISTER, {"worker_id": "CRASH-WORKER", "cores": 2}))
        recv_framed_message(s1)  # REGISTER_ACK
        send_framed_message(s1, ProtocolMessage.create(MessageType.STATUS, {"status": "READY"}))

        # Worker 1 menerima task
        task_msg = recv_framed_message(s1)
        self.assertEqual(task_msg["type"], MessageType.TASK.value)
        crashed_task_id = task_msg["payload"]["task_id"]

        # SIMULASI CRASH: Tutup socket mendadak tanpa kirim RESULT
        s1.close()
        time.sleep(0.5)  # Beri jeda agar Master mendeteksi disconnect dan me-requeue task

        # Verifikasi task kembali PENDING dengan retry_count bertambah
        task_obj = master.task_queue.get_task(crashed_task_id)
        self.assertEqual(task_obj.retry_count, 1)

        # Luncurkan Worker 2 (sehat) untuk menyelesaikan seluruh pekerjaan
        worker_healthy = WorkerNode(
            master_host="127.0.0.1",
            master_port=self.port,
            worker_id="HEALTHY-WORKER",
            cores=4
        )
        w2_thread = threading.Thread(target=worker_healthy.start, name="HealthyWorkerThread")
        w2_thread.start()

        # Tunggu Master selesai
        master_thread.join(timeout=10.0)
        self.assertFalse(master_thread.is_alive(), "Master tidak selesai!")
        w2_thread.join(timeout=5.0)

        # Periksa file hasil
        self.assertTrue(os.path.exists(self.output_json))
        with open(self.output_json, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.assertEqual(data["total_iterations"], TOTAL_ITERS)
        self.assertEqual(data["total_tasks_completed"], 2)
        # Seluruh task harus berhasil diselesaikan oleh HEALTHY-WORKER
        self.assertEqual(data["per_worker_contributions"]["HEALTHY-WORKER"], TOTAL_ITERS)

    def test_dynamic_load_balancing_strategy_hook(self):
        """
        Pengujian Dynamic Load Balancing Strategy Hook:
        Memastikan MasterController menerima strategi kustom tanpa error.
        """
        dlb_strategy = DynamicLoadBalancingStrategy()
        master = MasterController(
            host="127.0.0.1",
            port=self.port,
            total_iterations=1000,
            chunk_size=500,
            scheduler_strategy=dlb_strategy
        )
        self.assertIs(master.scheduler.strategy, dlb_strategy)

    def test_long_running_task_not_requeued_with_worker_heartbeat(self):
        """
        Worker mengerjakan task lebih lama dari timeout fault tolerance,
        namun tetap dianggap sehat karena heartbeat periodik dari worker.
        Task tidak boleh di-requeue dan tidak boleh dihitung ganda.
        """
        TOTAL_ITERS = 500
        CHUNK_SIZE = 500

        master = MasterController(
            host="127.0.0.1",
            port=self.port,
            total_iterations=TOTAL_ITERS,
            chunk_size=CHUNK_SIZE,
            min_workers=1,
            output_file=self.output_json,
            enable_fault_tolerance=True,
            worker_timeout_seconds=4.0
        )

        master_result_container = []

        def run_master():
            master_result_container.append(master.start(wait_timeout=20.0))

        master_thread = threading.Thread(target=run_master, name="Master-LongTask-Heartbeat")
        master_thread.start()
        time.sleep(0.1)

        def slow_simulation(payload, processes):
            time.sleep(5.0)  # Lebih lama dari worker_timeout_seconds
            return {
                "task_id": payload["task_id"],
                "worker_id": payload["worker_id"],
                "execution_time_seconds": 5.0,
                "iterations_completed": payload["iterations"],
                "simulation_summary": {
                    "peak_infected_mean": 1.0,
                    "peak_day_mean": 1.0,
                    "total_infected_mean": 1.0
                },
                "trajectories": {
                    "mean_s": [1.0],
                    "mean_i": [1.0],
                    "mean_r": [1.0]
                },
                "cores_used": processes,
            }

        with patch("worker.worker.execute_task", side_effect=slow_simulation):
            worker = WorkerNode(
                master_host="127.0.0.1",
                master_port=self.port,
                worker_id="HEARTBEAT-LONGTASK-WORKER",
                cores=2
            )
            worker_thread = threading.Thread(target=worker.start, name="Worker-LongTask-Heartbeat")
            worker_thread.start()

            master_thread.join(timeout=25.0)
            self.assertFalse(master_thread.is_alive(), "Master tidak selesai dalam batas waktu.")

            worker_thread.join(timeout=5.0)
            self.assertFalse(worker_thread.is_alive(), "Worker tidak berhenti bersih.")

        self.assertEqual(len(master_result_container), 1)
        self.assertIsNotNone(master_result_container[0])

        task = master.task_queue.get_task("TASK-0001")
        self.assertIsNotNone(task)
        self.assertEqual(task.retry_count, 0, "Task tidak boleh di-requeue jika heartbeat sehat.")

        summary = master.task_queue.get_summary()
        self.assertEqual(summary["completed"], 1)
        self.assertEqual(summary["pending"], 0)
        self.assertEqual(summary["running"], 0)

        self.assertTrue(os.path.exists(self.output_json))
        with open(self.output_json, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.assertEqual(data["total_iterations"], TOTAL_ITERS)
        self.assertEqual(data["total_tasks_completed"], 1)
        self.assertEqual(data["per_worker_contributions"].get("HEARTBEAT-LONGTASK-WORKER"), TOTAL_ITERS)


if __name__ == "__main__":
    unittest.main()
