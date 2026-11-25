"""
Unit Test: test_task_decomposer.py
Menguji keabsahan algoritma dekomposisi task, integritas pembagian iterasi,
keunikan task_id, dan independensi random seed.
"""

import sys
import os
import unittest

# Menambahkan direktori project root ke sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from master.task_decomposer import TaskDecomposer, Task, SimulationConfig


class TestTaskDecomposer(unittest.TestCase):

    def setUp(self):
        self.decomposer = TaskDecomposer(base_seed=42)
        self.config = SimulationConfig(
            population=10000,
            initial_infected=10,
            initial_recovered=0,
            beta=0.3,
            gamma=0.1,
            time_steps=100
        )

    def test_exact_division(self):
        """Uji jika total_iterations habis dibagi chunk_size."""
        total = 10000
        chunk = 1000
        tasks = self.decomposer.decompose(total, chunk, self.config)

        self.assertEqual(len(tasks), 10)
        self.assertEqual(sum(t.iterations for t in tasks), total)
        for t in tasks:
            self.assertEqual(t.iterations, 1000)

    def test_division_with_remainder(self):
        """Uji jika total_iterations memiliki sisa pembagian (remainder)."""
        total = 10007
        chunk = 1000
        tasks = self.decomposer.decompose(total, chunk, self.config)

        self.assertEqual(len(tasks), 10)
        # Total akumulasi harus tepat 10007 (tidak ada sisa yang hilang)
        self.assertEqual(sum(t.iterations for t in tasks), total)
        
        # 7 task pertama harus memiliki 1001 iterasi, 3 sisanya 1000 iterasi
        iterations_list = [t.iterations for t in tasks]
        self.assertEqual(iterations_list.count(1001), 7)
        self.assertEqual(iterations_list.count(1000), 3)

    def test_total_smaller_than_chunk(self):
        """Uji jika total iterasi lebih kecil dari ukuran chunk."""
        total = 50
        chunk = 500
        tasks = self.decomposer.decompose(total, chunk, self.config)

        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].iterations, 50)
        self.assertEqual(tasks[0].task_id, "TASK-0001")

    def test_unique_task_ids(self):
        """Pastikan setiap task_id unik dan konsisten."""
        tasks = self.decomposer.decompose(5000, 500, self.config)
        task_ids = [t.task_id for t in tasks]

        self.assertEqual(len(task_ids), len(set(task_ids)), "Terdapat task_id yang duplikat!")

    def test_unique_random_seeds(self):
        """Pastikan setiap task memiliki random seed unik agar Monte Carlo tidak bias."""
        tasks = self.decomposer.decompose(5000, 500, self.config)
        seeds = [t.random_seed for t in tasks]

        self.assertEqual(len(seeds), len(set(seeds)), "Terdapat random_seed yang identik!")

    def test_invalid_parameters_raise_error(self):
        """Uji penanganan parameter tidak valid."""
        with self.assertRaises(ValueError):
            self.decomposer.decompose(0, 100, self.config)

        with self.assertRaises(ValueError):
            self.decomposer.decompose(1000, 0, self.config)

        # Invalid config
        bad_config = SimulationConfig(beta=1.5)  # beta > 1.0
        with self.assertRaises(ValueError):
            self.decomposer.decompose(1000, 100, bad_config)

    def test_task_serialization_to_dict(self):
        """Uji konversi task ke dict (JSON-ready) dan rekonstruksi kembali."""
        tasks = self.decomposer.decompose(1000, 1000, self.config)
        original_task = tasks[0]

        task_dict = original_task.to_dict()
        self.assertIsInstance(task_dict, dict)
        self.assertEqual(task_dict["task_id"], original_task.task_id)
        self.assertEqual(task_dict["iterations"], original_task.iterations)

        # Rekonstruksi
        reconstructed = Task.from_dict(task_dict)
        self.assertEqual(reconstructed.task_id, original_task.task_id)
        self.assertEqual(reconstructed.random_seed, original_task.random_seed)


if __name__ == "__main__":
    unittest.main()
