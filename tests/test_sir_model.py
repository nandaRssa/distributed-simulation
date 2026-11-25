"""Unit test model SIR stokastik untuk Anggota 2."""

import sys
import os
import unittest

import numpy as np

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from simulation.sir_model import run_sir_once
from simulation.simulation_config import SimulationConfig



class TestSirModel(unittest.TestCase):

    def test_population_conservation_and_non_negative(self):
        """Memastikan S+I+R tetap konstan dan seluruh kompartemen tidak negatif."""
        cfg = SimulationConfig(
            population=5000,
            initial_infected=20,
            initial_recovered=5,
            beta=0.28,
            gamma=0.08,
            time_steps=120,
        )

        out = run_sir_once(cfg, seed=12345)
        s = out["s"]
        i = out["i"]
        r = out["r"]

        self.assertTrue(np.all(s >= 0.0))
        self.assertTrue(np.all(i >= 0.0))
        self.assertTrue(np.all(r >= 0.0))

        total = s + i + r
        self.assertTrue(np.allclose(total, cfg.population))

    def test_same_seed_same_output(self):
        """Memastikan seed sama menghasilkan lintasan identik."""
        cfg = SimulationConfig(
            population=4000,
            initial_infected=15,
            initial_recovered=0,
            beta=0.25,
            gamma=0.1,
            time_steps=90,
        )

        out_a = run_sir_once(cfg, seed=2026)
        out_b = run_sir_once(cfg, seed=2026)

        self.assertTrue(np.array_equal(out_a["s"], out_b["s"]))
        self.assertTrue(np.array_equal(out_a["i"], out_b["i"]))
        self.assertTrue(np.array_equal(out_a["r"], out_b["r"]))
        self.assertEqual(out_a["peak_infected"], out_b["peak_infected"])
        self.assertEqual(out_a["peak_day"], out_b["peak_day"])
        self.assertEqual(out_a["total_infected"], out_b["total_infected"])


if __name__ == "__main__":
    unittest.main()
