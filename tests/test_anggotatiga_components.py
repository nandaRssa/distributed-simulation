"""
Unit Test: test_anggotatiga_components.py
Fokus: Menguji fungsionalitas seluruh modul yang dikembangkan Anggota 3:
1. DynamicLoadBalancer & WorkerPerformanceProfile (master/load_balancer.py)
2. FaultToleranceManager (master/fault_tolerance.py)
3. PerformanceMonitor (master/performance_monitor.py)
4. Modul Visualisasi (visualization/epidemic_chart.py & performance_chart.py)
"""

import sys
import os
import unittest
import tempfile
import json
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from master.load_balancer import DynamicLoadBalancer, WorkerPerformanceProfile
from master.fault_tolerance import FaultToleranceManager
from master.performance_monitor import PerformanceMonitor
from master.task_queue import TaskQueue, TaskStatus
from master.worker_manager import WorkerManager
from master.task_decomposer import Task
from visualization.epidemic_chart import plot_epidemic_curves
from visualization.performance_chart import plot_speedup_and_efficiency, plot_throughput_and_execution_time


class TestAnggotaTigaModules(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_dynamic_load_balancer(self):
        """Memvalidasi registrasi worker, pencatatan throughput, dan pemilihan task."""
        lb = DynamicLoadBalancer()
        lb.register_worker("W1", cpu_cores=4)
        lb.register_worker("W2", cpu_cores=8)

        # Catat penyelesaian task W1 (lambat) vs W2 (cepat)
        lb.record_task_result("W1", iterations=1000, duration_seconds=1.0)  # 1000 iter/s
        lb.record_task_result("W2", iterations=1000, duration_seconds=0.25) # 4000 iter/s

        summary = lb.get_load_distribution_summary()
        self.assertEqual(summary["W1"]["tasks_completed"], 1)
        self.assertEqual(summary["W2"]["tasks_completed"], 1)
        self.assertGreater(summary["W2"]["estimated_throughput_iter_per_sec"], summary["W1"]["estimated_throughput_iter_per_sec"])

        # Test select_task
        tq = TaskQueue()
        t1 = Task(task_id="T1", iterations=500, simulation_config={}, random_seed=1)
        tq.add_task(t1)

        chosen_task = lb.select_task(tq, "W2")
        self.assertIsNotNone(chosen_task)
        self.assertEqual(chosen_task.task_id, "T1")

    def test_fault_tolerance_max_retries_and_blacklist(self):
        """Memvalidasi batas maksimal retry dan blacklisting worker bandel."""
        tq = TaskQueue()
        wm = WorkerManager()

        t1 = Task(task_id="TASK-FAIL", iterations=500, simulation_config={}, random_seed=42)
        tq.add_task(t1)

        ft = FaultToleranceManager(task_queue=tq, worker_manager=wm, max_task_retries=2)

        # Simulasikan kegagalan 3 kali pada worker yang sama
        for i in range(3):
            # Worker mengambil task
            wm.register_worker(worker_id="FLAKY-WORKER", ip="127.0.0.1", port=5000 + i)
            task = tq.get_next_task("FLAKY-WORKER")
            if task:
                wm.assign_task("FLAKY-WORKER", task.task_id)

            # Worker crash / disconnected
            wm.mark_disconnected("FLAKY-WORKER")
            recovered = ft.check_and_recover()

        # Setelah 3 kali gagal, worker harus masuk blacklist
        summary = ft.get_summary()
        self.assertIn("FLAKY-WORKER", summary["blacklisted_workers"])
        self.assertGreaterEqual(summary["total_worker_failures"], 3)

    def test_performance_monitor_metrics(self):
        """Memvalidasi penghitungan wall-clock, throughput, speedup, dan efisiensi."""
        monitor = PerformanceMonitor(sample_interval_seconds=0.05)
        monitor.start()
        time.sleep(0.15)
        monitor.record_recovery_event("T1", "W-CRASH", 0.05)
        monitor.stop()

        metrics = monitor.compute_evaluation_metrics(
            total_iterations=10000,
            sequential_baseline_time=2.0,
            num_processing_units=4
        )

        self.assertGreater(metrics["wall_clock_time_seconds"], 0)
        self.assertGreater(metrics["throughput_iterations_per_sec"], 0)
        self.assertIsNotNone(metrics["speedup"])
        self.assertIsNotNone(metrics["efficiency"])
        self.assertEqual(metrics["fault_recovery"]["total_recovery_events"], 1)

    def test_visualization_generation(self):
        """Memvalidasi pembuatan grafik PNG tanpa error GUI headless."""
        # 1. Dummy simulation result JSON
        sim_json = os.path.join(self.temp_dir.name, "sim_test.json")
        epidemic_png = os.path.join(self.temp_dir.name, "epidemic.png")
        dummy_sim = {
            "total_iterations": 1000,
            "trajectories": {
                "mean_s": [900, 800, 700, 600],
                "mean_i": [100, 150, 120, 50],
                "mean_r": [0, 50, 180, 350]
            },
            "summary": {"peak_infected_mean": 150.0, "peak_day_mean": 1}
        }
        with open(sim_json, "w", encoding="utf-8") as f:
            json.dump(dummy_sim, f)

        res_path = plot_epidemic_curves(sim_json, epidemic_png)
        self.assertTrue(os.path.exists(epidemic_png))

        # 2. Dummy scalability results JSON
        scale_json = os.path.join(self.temp_dir.name, "scale_test.json")
        speedup_png = os.path.join(self.temp_dir.name, "speedup.png")
        tp_png = os.path.join(self.temp_dir.name, "throughput.png")
        dummy_scale = {
            "experiments": [
                {"num_workers": 1, "wall_clock_time_seconds": 10.0, "throughput_iterations_per_sec": 1000, "speedup": 1.0, "efficiency": 1.0},
                {"num_workers": 2, "wall_clock_time_seconds": 5.2, "throughput_iterations_per_sec": 1923, "speedup": 1.92, "efficiency": 0.96},
                {"num_workers": 4, "wall_clock_time_seconds": 2.8, "throughput_iterations_per_sec": 3571, "speedup": 3.57, "efficiency": 0.89}
            ]
        }
        with open(scale_json, "w", encoding="utf-8") as f:
            json.dump(dummy_scale, f)

        p1 = plot_speedup_and_efficiency(scale_json, speedup_png)
        p2 = plot_throughput_and_execution_time(scale_json, tp_png)

        self.assertTrue(os.path.exists(speedup_png))
        self.assertTrue(os.path.exists(tp_png))


if __name__ == "__main__":
    unittest.main()

