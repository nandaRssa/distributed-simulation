"""
Unit Test: test_worker_manager.py
Menguji fungsionalitas WorkerManager: registrasi multi-worker (hingga 9+ node),
pelacakan status koneksi, alokasi & penyelesaian task, deteksi timeout / dead node (Anggota 3),
serta konsistensi data multi-thread.
"""

import sys
import os
import unittest
import time
import threading

# Menambahkan direktori project root ke sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from master.worker_manager import WorkerManager, WorkerInfo


class TestWorkerManager(unittest.TestCase):

    def setUp(self):
        self.manager = WorkerManager()

    def test_register_multiple_workers(self):
        """Uji registrasi 9 node worker fisik sesuai spesifikasi UAS."""
        for i in range(1, 10):
            w_id = f"WORKER-NODE-{i:02d}"
            ip = f"192.168.1.{10 + i}"
            port = 50000 + i
            self.manager.register_worker(
                worker_id=w_id,
                ip=ip,
                port=port,
                cpu_cores=8,
                metadata={"hostname": f"lab-pc-{i:02d}"}
            )

        summary = self.manager.get_summary()
        self.assertEqual(summary["total_registered"], 9)
        self.assertEqual(summary["connected"], 9)
        self.assertEqual(summary["ready"], 9)
        self.assertEqual(summary["busy"], 0)

        # Cek lookup berdasarkan alamat IP & Port
        found = self.manager.get_worker_by_address(("192.168.1.15", 50005))
        self.assertIsNotNone(found)
        self.assertEqual(found.worker_id, "WORKER-NODE-05")

    def test_assign_and_complete_task(self):
        """Uji alokasi task dan pencatatan metrik waktu eksekusi worker."""
        self.manager.register_worker("W-01", "192.168.1.20", 5001, cpu_cores=4)

        # Alokasikan task
        self.manager.assign_task("W-01", "TASK-100")
        worker = self.manager.get_worker("W-01")
        self.assertEqual(worker.work_status, "BUSY")
        self.assertEqual(worker.current_task_id, "TASK-100")

        # Selesaikan task
        self.manager.complete_task("W-01", "TASK-100", execution_time=3.5)
        self.assertEqual(worker.work_status, "READY")
        self.assertIsNone(worker.current_task_id)
        self.assertEqual(worker.completed_tasks_count, 1)
        self.assertEqual(worker.total_execution_time, 3.5)

    def test_disconnect_and_reconnect(self):
        """Uji saat worker terputus dan melakukan rekoneksi kembali."""
        self.manager.register_worker("W-PERSIST", "192.168.1.30", 5002)
        self.manager.complete_task("W-PERSIST", "TASK-1", execution_time=2.0)

        # Worker terputus
        self.manager.mark_disconnected(("192.168.1.30", 5002))
        w = self.manager.get_worker("W-PERSIST")
        self.assertEqual(w.connection_status, "DISCONNECTED")
        self.assertEqual(w.work_status, "OFFLINE")
        # Histori selesai harus tetap terjaga
        self.assertEqual(w.completed_tasks_count, 1)

        # Worker rekoneksi dengan IP/Port baru
        self.manager.register_worker("W-PERSIST", "192.168.1.31", 5003)
        w_recon = self.manager.get_worker("W-PERSIST")
        self.assertEqual(w_recon.connection_status, "CONNECTED")
        self.assertEqual(w_recon.work_status, "READY")
        self.assertEqual(w_recon.completed_tasks_count, 1)

    def test_timeout_dead_node_detection_for_anggota_3(self):
        """Uji fungsionalitas pendeteksi timeout untuk modul Fault Tolerance milik Anggota 3."""
        self.manager.register_worker("W-ACTIVE", "192.168.1.40", 5004)
        self.manager.register_worker("W-STALE", "192.168.1.41", 5005)

        # Simulasi W-STALE memiliki last_seen masa lalu (10 detik lalu)
        stale_worker = self.manager.get_worker("W-STALE")
        stale_worker.last_seen = time.time() - 10.0

        # Cari worker yang tidak aktif selama > 5 detik
        timed_out = self.manager.get_timed_out_workers(timeout_threshold_seconds=5.0)
        timed_out_ids = [w.worker_id for w in timed_out]

        self.assertIn("W-STALE", timed_out_ids)
        self.assertNotIn("W-ACTIVE", timed_out_ids)

    def test_thread_safety_multi_worker_registration(self):
        """Uji konkurensi: banyak worker mendaftar dan mengupdate status secara bersamaan."""
        NUM_WORKERS = 20

        def register_routine(idx):
            w_id = f"THREAD-W-{idx}"
            self.manager.register_worker(w_id, "127.0.0.1", 10000 + idx)
            self.manager.assign_task(w_id, f"T-{idx}")
            self.manager.complete_task(w_id, f"T-{idx}", 1.0)

        threads = [threading.Thread(target=register_routine, args=(i,)) for i in range(NUM_WORKERS)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

        summary = self.manager.get_summary()
        self.assertEqual(summary["total_registered"], NUM_WORKERS)
        self.assertEqual(summary["ready"], NUM_WORKERS)


if __name__ == "__main__":
    unittest.main()
