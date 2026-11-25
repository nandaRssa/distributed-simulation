"""Unit test kecil untuk parsing argumen CLI Master."""

import sys
import os
import unittest

# Menambahkan direktori project root ke sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from master.master import parse_args, build_simulation_config_from_args, resolve_total_iterations


class TestMasterCliArgs(unittest.TestCase):

    def test_preset_heavy_with_explicit_overrides(self):
        """Argumen eksplisit harus menimpa nilai preset heavy."""
        args = parse_args([
            "--preset", "heavy",
            "--population", "22222",
            "--time-steps", "150",
            "--initial-infected", "33",
            "--beta", "0.31",
            "--gamma", "0.12",
        ])

        cfg = build_simulation_config_from_args(args)

        self.assertEqual(cfg.population, 22222)
        self.assertEqual(cfg.time_steps, 150)
        self.assertEqual(cfg.initial_infected, 33)
        self.assertAlmostEqual(cfg.beta, 0.31)
        self.assertAlmostEqual(cfg.gamma, 0.12)

        # iterations tidak diisi eksplisit, jadi mengikuti preset heavy.
        self.assertEqual(resolve_total_iterations(args), 120000)


if __name__ == "__main__":
    unittest.main()
