"""Unit test Monte Carlo chunk runner untuk Anggota 2."""

import sys
import os
import unittest

import numpy as np

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from simulation.monte_carlo import run_chunk
from simulation.simulation_config import SimulationConfig


class TestMonteCarlo(unittest.TestCase):

    def test_error_shrinks_when_iterations_increase(self):
        """Selisih terhadap referensi Monte Carlo besar mengecil saat iterasi diperbesar."""
        cfg = SimulationConfig(
            population=3000,
            initial_infected=12,
            initial_recovered=0,
            beta=0.24,
            gamma=0.08,
            time_steps=80,
        )

        reference = run_chunk(cfg, iterations=12000, seed=123)
        ref_i = np.array(reference["trajectories"]["mean_i"], dtype=np.float64)

        small = run_chunk(cfg, iterations=200, seed=123)
        large = run_chunk(cfg, iterations=4000, seed=123)

        small_i = np.array(small["trajectories"]["mean_i"], dtype=np.float64)
        large_i = np.array(large["trajectories"]["mean_i"], dtype=np.float64)

        small_error = float(np.mean(np.abs(small_i - ref_i)))
        large_error = float(np.mean(np.abs(large_i - ref_i)))

        self.assertLess(large_error, small_error)

        # Validasi format keluaran tetap sesuai kontrak.
        self.assertIn("simulation_summary", large)
        self.assertIn("trajectories", large)
        self.assertEqual(len(large["trajectories"]["mean_s"]), cfg.time_steps + 1)
        self.assertEqual(len(large["trajectories"]["mean_i"]), cfg.time_steps + 1)
        self.assertEqual(len(large["trajectories"]["mean_r"]), cfg.time_steps + 1)


if __name__ == "__main__":
    unittest.main()
