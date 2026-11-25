"""Unit test engine multiprocessing simulasi untuk Anggota 2."""

import sys
import os
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from simulation.monte_carlo import run_chunk
from worker.multiprocessing_engine import MultiprocessingSimulationEngine
from worker.simulation_engine import execute_task


class TestMultiprocessingEngine(unittest.TestCase):

    def test_multiprocessing_statistically_consistent_with_sequential(self):
        """Hasil multiprocessing harus konsisten secara statistik terhadap sequential."""
        config = {
            "population": 5000,
            "initial_infected": 10,
            "initial_recovered": 0,
            "beta": 0.3,
            "gamma": 0.1,
            "time_steps": 100,
        }
        iterations = 1500
        seed = 77

        sequential = run_chunk(config=config, iterations=iterations, seed=seed)

        engine = MultiprocessingSimulationEngine(processes=2)
        try:
            engine.start()
            parallel = engine.run_task(config=config, iterations=iterations, seed=seed)
        finally:
            engine.stop()

        seq_summary = sequential["simulation_summary"]
        par_summary = parallel["simulation_summary"]

        self.assertAlmostEqual(seq_summary["peak_infected_mean"], par_summary["peak_infected_mean"], delta=40.0)
        self.assertAlmostEqual(seq_summary["peak_day_mean"], par_summary["peak_day_mean"], delta=5.0)
        self.assertAlmostEqual(seq_summary["total_infected_mean"], par_summary["total_infected_mean"], delta=60.0)

    def test_execute_task_reports_total_iterations_completed(self):
        """Payload hasil wajib melaporkan iterations_completed sesuai task input."""
        payload = {
            "task_id": "TASK-MP-0001",
            "worker_id": "WORKER-MP-01",
            "iterations": 600,
            "simulation_config": {
                "population": 2000,
                "initial_infected": 8,
                "initial_recovered": 0,
                "beta": 0.2,
                "gamma": 0.09,
                "time_steps": 60,
            },
            "random_seed": 2026,
        }

        result = execute_task(payload, processes=2)

        self.assertEqual(result["task_id"], payload["task_id"])
        self.assertEqual(result["worker_id"], payload["worker_id"])
        self.assertEqual(result["iterations_completed"], payload["iterations"])
        self.assertIn("simulation_summary", result)
        self.assertIn("trajectories", result)
        self.assertEqual(len(result["trajectories"]["mean_s"]), payload["simulation_config"]["time_steps"] + 1)


if __name__ == "__main__":
    unittest.main()
