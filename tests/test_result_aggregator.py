"""
Unit Test: test_result_aggregator.py
Menguji fungsionalitas ResultAggregator: validasi skema payload, pencegahan duplikasi (idempotensi),
identifikasi task yang belum selesai, kalkulasi rata-rata tertimbang (weighted average),
serta penyimpanan hasil ke format JSON.
"""

import sys
import os
import unittest
import json
import tempfile

# Menambahkan direktori project root ke sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from master.result_aggregator import ResultAggregator, AggregatedSimulationResult


class TestResultAggregator(unittest.TestCase):

    def setUp(self):
        self.expected_tasks = ["TASK-001", "TASK-002", "TASK-003"]
        self.aggregator = ResultAggregator(
            expected_task_ids=self.expected_tasks,
            total_expected_iterations=4000
        )

    def _sample_payload(self, task_id: str, worker_id: str, iters: int, peak: float, s_val: float):
        return {
            "task_id": task_id,
            "worker_id": worker_id,
            "execution_time_seconds": 1.5,
            "iterations_completed": iters,
            "simulation_summary": {
                "peak_infected_mean": peak,
                "peak_day_mean": 25.0,
                "total_infected_mean": 8000.0
            },
            "trajectories": {
                "mean_s": [s_val, s_val - 100],
                "mean_i": [10.0, 110.0],
                "mean_r": [0.0, 50.0]
            }
        }

    def test_add_valid_result(self):
        """Uji penambahan hasil yang valid dan deteksi kelengkapan."""
        payload1 = self._sample_payload("TASK-001", "W-1", 1000, 100.0, 9900.0)
        success = self.aggregator.add_result("TASK-001", payload1)
        self.assertTrue(success)
        self.assertFalse(self.aggregator.is_complete())
        self.assertEqual(len(self.aggregator.get_missing_task_ids()), 2)

    def test_reject_unknown_task_id(self):
        """Uji penolakan task_id yang tidak ada dalam daftar yang diharapkan."""
        payload = self._sample_payload("TASK-UNKNOWN", "W-1", 1000, 100.0, 9900.0)
        success = self.aggregator.add_result("TASK-UNKNOWN", payload)
        self.assertFalse(success)

    def test_reject_malformed_payload(self):
        """Uji penolakan payload yang tidak lengkap (merusak kontrak data)."""
        bad_payload = {
            "task_id": "TASK-001",
            "worker_id": "W-1"
            # Missing iterations_completed & trajectories
        }
        success = self.aggregator.add_result("TASK-001", bad_payload)
        self.assertFalse(success)

    def test_idempotency_prevent_duplicate_aggregation(self):
        """Uji pencegahan perhitungan ganda jika task yang sama dikirim dua kali."""
        payload = self._sample_payload("TASK-001", "W-1", 1000, 100.0, 9900.0)
        
        # Kirim pertama -> diterima
        self.assertTrue(self.aggregator.add_result("TASK-001", payload))
        self.assertEqual(self.aggregator.get_received_count(), 1)

        # Kirim ulang (duplikat jaringan) -> ditolak
        self.assertFalse(self.aggregator.add_result("TASK-001", payload))
        self.assertEqual(self.aggregator.get_received_count(), 1)

    def test_weighted_average_calculation(self):
        """
        Uji matematis perhitungan weighted average.
        Task 1: 1000 iter, peak = 100, S = [9000, 8000]
        Task 2: 3000 iter, peak = 200, S = [7000, 6000]
        Total = 4000 iter.
        Weight: T1 = 0.25, T2 = 0.75.
        Diharapkan:
        peak = 0.25 * 100 + 0.75 * 200 = 25 + 150 = 175.
        S[0] = 0.25 * 9000 + 0.75 * 7000 = 2250 + 5250 = 7500.
        S[1] = 0.25 * 8000 + 0.75 * 6000 = 2000 + 4500 = 6500.
        """
        agg = ResultAggregator(["TASK-001", "TASK-002"], total_expected_iterations=4000)
        p1 = self._sample_payload("TASK-001", "W-1", 1000, 100.0, 9000.0)
        p2 = self._sample_payload("TASK-002", "W-2", 3000, 200.0, 7000.0)

        agg.add_result("TASK-001", p1)
        agg.add_result("TASK-002", p2)
        self.assertTrue(agg.is_complete())

        final_res = agg.aggregate()
        self.assertIsNotNone(final_res)
        self.assertEqual(final_res.total_iterations, 4000)
        self.assertEqual(final_res.summary["peak_infected_mean"], 175.0)
        self.assertEqual(final_res.trajectories["mean_s"], [7500.0, 7400.0])
        self.assertEqual(final_res.per_worker_contributions["W-1"], 1000)
        self.assertEqual(final_res.per_worker_contributions["W-2"], 3000)

    def test_export_to_json(self):
        """Uji ekspor hasil agregasi ke file JSON di disk."""
        agg = ResultAggregator(["TASK-001"], total_expected_iterations=1000)
        p1 = self._sample_payload("TASK-001", "W-1", 1000, 150.0, 9000.0)
        agg.add_result("TASK-001", p1)

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            agg.export_to_json(tmp_path)
            self.assertTrue(os.path.exists(tmp_path))

            with open(tmp_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            self.assertEqual(data["total_iterations"], 1000)
            self.assertIn("trajectories", data)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)


if __name__ == "__main__":
    unittest.main()
