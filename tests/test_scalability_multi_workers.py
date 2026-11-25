"""
Scalability Test: test_scalability_multi_workers.py
Menguji skalabilitas Master terhadap variasi jumlah Worker:
1, 2, 4, 6, dan 9 Worker (sesuai spesifikasi wajib UAS: 1 Master + 9 Worker fisik).
Memvalidasi throughput, pembagian beban kerja merata, dan penggabungan hasil.
"""

import sys
import os
import unittest
import socket
import threading
import time
import tempfile
import json
from typing import List

# Menambahkan direktori project root ke sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from master.master import MasterController
from worker.worker import WorkerNode


class TestScalabilityMultiWorkers(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.temp_dir.cleanup()

    def _get_free_port(self) -> int:
        temp_s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        temp_s.bind(("127.0.0.1", 0))
        port = temp_s.getsockname()[1]
        temp_s.close()
        return port

    def _run_scalability_experiment(self, num_workers: int) -> dict:
        port = self._get_free_port()
        output_file = os.path.join(self.temp_dir.name, f"result_workers_{num_workers}.json")
        TOTAL_ITERS = 9000
        CHUNK_SIZE = 500  # 18 tasks total

        master = MasterController(
            host="127.0.0.1",
            port=port,
            total_iterations=TOTAL_ITERS,
            chunk_size=CHUNK_SIZE,
            min_workers=num_workers,
            output_file=output_file,
            enable_fault_tolerance=True
        )

        master_thread = threading.Thread(target=master.start, args=(25.0,), name=f"Master-{num_workers}W")
        master_thread.start()
        time.sleep(0.1)

        workers: List[WorkerNode] = []
        worker_threads: List[threading.Thread] = []

        for i in range(1, num_workers + 1):
            w = WorkerNode(
                master_host="127.0.0.1",
                master_port=port,
                worker_id=f"PHYSICAL-NODE-{i:02d}",
                cores=2
            )
            workers.append(w)
            th = threading.Thread(target=w.start, name=f"WorkerNode-{i:02d}")
            worker_threads.append(th)
            th.start()

        master_thread.join(timeout=30.0)
        self.assertFalse(master_thread.is_alive(), f"Master tidak selesai pada uji {num_workers} worker!")

        for th in worker_threads:
            th.join(timeout=5.0)

        self.assertTrue(os.path.exists(output_file), f"Output file tidak ada untuk {num_workers} worker!")
        with open(output_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        return data

    def test_scalability_1_worker(self):
        """Uji dengan 1 Worker."""
        data = self._run_scalability_experiment(num_workers=1)
        self.assertEqual(data["total_iterations"], 9000)
        self.assertEqual(len(data["per_worker_contributions"]), 1)

    def test_scalability_2_workers(self):
        """Uji dengan 2 Worker."""
        data = self._run_scalability_experiment(num_workers=2)
        self.assertEqual(data["total_iterations"], 9000)
        self.assertEqual(len(data["per_worker_contributions"]), 2)

    def test_scalability_4_workers(self):
        """Uji dengan 4 Worker."""
        data = self._run_scalability_experiment(num_workers=4)
        self.assertEqual(data["total_iterations"], 9000)
        self.assertEqual(len(data["per_worker_contributions"]), 4)

    def test_scalability_6_workers(self):
        """Uji dengan 6 Worker."""
        data = self._run_scalability_experiment(num_workers=6)
        self.assertEqual(data["total_iterations"], 9000)
        self.assertEqual(len(data["per_worker_contributions"]), 6)

    def test_scalability_9_workers_full_physical_target(self):
        """Uji dengan konfigurasi maksimal UAS: 9 Worker simultan (1 Master + 9 Worker)."""
        data = self._run_scalability_experiment(num_workers=9)
        self.assertEqual(data["total_iterations"], 9000)
        self.assertEqual(data["total_tasks_completed"], 18)
        # Pastikan seluruh 9 node worker berkontribusi aktif
        self.assertEqual(len(data["per_worker_contributions"]), 9)
        self.assertEqual(sum(data["per_worker_contributions"].values()), 9000)


if __name__ == "__main__":
    unittest.main()
