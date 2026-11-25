"""
Integration Test: test_end_to_end_integration.py
Menguji integrasi end-to-end secara penuh antara MasterController dan beberapa node Worker
melalui jaringan TCP Socket nyata. Memverifikasi pembagian 10 task, pemrosesan simultan 3 worker,
pengumpulan seluruh hasil, penggabungan kurva SIR, dan graceful shutdown.
"""

import sys
import os
import unittest
import socket
import threading
import time
import json
import tempfile

# Menambahkan direktori project root ke sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from master.master import MasterController
from worker.worker import WorkerNode


class TestEndToEndIntegration(unittest.TestCase):

    def setUp(self):
        # Cari port bebas
        temp_s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        temp_s.bind(("127.0.0.1", 0))
        self.port = temp_s.getsockname()[1]
        temp_s.close()

        # Temporary output file
        self.temp_dir = tempfile.TemporaryDirectory()
        self.output_json = os.path.join(self.temp_dir.name, "integration_result.json")

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_full_simulation_flow_with_three_workers(self):
        """
        Uji end-to-end lengkap:
        - Master: 5.000 iterasi dibagi menjadi 10 task (@ 500 iterasi).
        - Menunggu minimal 3 Worker terhubung.
        - 3 Worker fisik (disimulasikan dengan 3 proses thread independen) terhubung dan bersaing mengambil tugas.
        - Master mengakumulasi seluruh hasil dan menyimpan ke file JSON.
        """
        TOTAL_ITERS = 5000
        CHUNK_SIZE = 500
        NUM_WORKERS = 3

        master = MasterController(
            host="127.0.0.1",
            port=self.port,
            total_iterations=TOTAL_ITERS,
            chunk_size=CHUNK_SIZE,
            min_workers=NUM_WORKERS,
            output_file=self.output_json
        )

        # Thread untuk menjalankan Master
        master_result_container = []

        def run_master():
            res = master.start(wait_timeout=20.0)
            master_result_container.append(res)

        master_thread = threading.Thread(target=run_master, name="Master-Runner")
        master_thread.start()

        # Beri jeda 0.1 detik agar master listener aktif
        time.sleep(0.1)

        # Luncurkan 3 Worker
        worker_threads = []
        workers = []
        for i in range(1, NUM_WORKERS + 1):
            w = WorkerNode(
                master_host="127.0.0.1",
                master_port=self.port,
                worker_id=f"TEST-WORKER-{i}",
                cores=2
            )
            workers.append(w)
            th = threading.Thread(target=w.start, name=f"WorkerThread-{i}")
            worker_threads.append(th)
            th.start()

        # Tunggu Master selesai
        master_thread.join(timeout=25.0)
        self.assertFalse(master_thread.is_alive(), "Master tidak selesai dalam batas waktu!")

        # Tunggu seluruh Worker berhenti
        for th in worker_threads:
            th.join(timeout=5.0)
            self.assertFalse(th.is_alive(), "Worker tidak berhenti secara bersih!")

        # Validasi Hasil
        self.assertEqual(len(master_result_container), 1)
        res = master_result_container[0]
        self.assertIsNotNone(res, "Master tidak menghasilkan AggregatedSimulationResult!")

        self.assertEqual(res.total_iterations, TOTAL_ITERS)
        self.assertEqual(res.total_tasks_completed, 10)
        self.assertTrue(os.path.exists(self.output_json), "File output JSON tidak terbentuk!")

        # Baca file JSON
        with open(self.output_json, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.assertEqual(data["total_iterations"], TOTAL_ITERS)
        self.assertEqual(data["total_tasks_completed"], 10)
        self.assertIn("trajectories", data)
        self.assertIn("mean_s", data["trajectories"])
        self.assertIn("mean_i", data["trajectories"])
        self.assertIn("mean_r", data["trajectories"])

        # Verifikasi kontribusi seluruh worker (ketiganya harus mendapatkan task)
        contributions = data["per_worker_contributions"]
        self.assertEqual(len(contributions), NUM_WORKERS, "Tidak semua worker mendapatkan bagian task!")
        self.assertEqual(sum(contributions.values()), TOTAL_ITERS)


if __name__ == "__main__":
    unittest.main()
