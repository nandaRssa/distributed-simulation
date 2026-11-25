"""
Benchmark: local_parallel.py
Fokus: Pengujian komputasi paralel lokal menggunakan multiprocessing
       pada 1 komputer dengan variasi jumlah core / proses (1, 2, 4, 8 proses).
Bagian dari: System Optimization & Performance Evaluation (Anggota 3)
"""

import sys
import os
import time
import json
import logging
import argparse
import multiprocessing as mp
from typing import Dict, Any, List

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from simulation.simulation_config import SimulationConfig, normalize_config
from worker.multiprocessing_engine import MultiprocessingSimulationEngine
from master.performance_monitor import PerformanceMonitor

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [LocalParallelBenchmark]: %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("LocalParallelBenchmark")


def run_single_local_parallel(
    processes: int,
    iterations: int = 10000,
    config: Any = None,
    seed: int = 42
) -> Dict[str, Any]:
    """Menjalankan satu uji coba multiprocessing lokal dengan jumlah proses tertentu."""
    cfg = normalize_config(config or SimulationConfig())
    engine = MultiprocessingSimulationEngine(processes=processes)
    engine.start()

    monitor = PerformanceMonitor(sample_interval_seconds=0.2)
    monitor.start()

    t_start = time.time()
    result = engine.run_task(config=cfg, iterations=iterations, seed=seed)
    t_end = time.time()

    monitor.stop()
    engine.stop()

    wall_clock = t_end - t_start
    throughput = (iterations / wall_clock) if wall_clock > 0 else 0.0
    res_stats = monitor.get_resource_summary()

    return {
        "processes": processes,
        "iterations": iterations,
        "wall_clock_time_seconds": round(wall_clock, 4),
        "throughput_iterations_per_sec": round(throughput, 2),
        "cpu_utilization": {
            "avg_percent": res_stats["avg_cpu_percent"],
            "peak_percent": res_stats["peak_cpu_percent"]
        },
        "memory_utilization": {
            "avg_mb": res_stats["avg_memory_mb"],
            "peak_mb": res_stats["peak_memory_mb"]
        }
    }


def run_local_parallel_suite(
    process_counts: List[int] = None,
    iterations: int = 10000,
    output_file: str = "results/benchmark_local_parallel.json"
) -> Dict[str, Any]:
    """Menjalankan rangkaian uji multiprocessing lokal untuk berbagai variasi core."""
    if process_counts is None:
        max_cpus = os.cpu_count() or 4
        # Contoh variasi: 1, 2, 4, min(8, max_cpus)
        candidates = [1, 2, 4, 8]
        process_counts = sorted(list(set([p for p in candidates if p <= max(4, max_cpus)])))

    logger.info(f"Memulai Local Parallel Benchmark: variasi proses {process_counts}, {iterations} iterasi...")

    results: List[Dict[str, Any]] = []
    t_seq: float = 0.0

    for p in process_counts:
        logger.info(f"--- Menjalankan uji dengan {p} proses paralel ---")
        run_data = run_single_local_parallel(processes=p, iterations=iterations)
        
        if p == 1 or t_seq == 0.0:
            t_seq = run_data["wall_clock_time_seconds"]

        # Hitung Speedup dan Efisiensi lokal
        speedup = round(t_seq / run_data["wall_clock_time_seconds"], 3) if run_data["wall_clock_time_seconds"] > 0 else 1.0
        efficiency = round(speedup / p, 4)

        run_data["speedup"] = speedup
        run_data["efficiency"] = efficiency
        results.append(run_data)

        logger.info(
            f"Processes: {p} | Time: {run_data['wall_clock_time_seconds']}s | "
            f"Speedup: {speedup:.2f}x | Efficiency: {efficiency * 100:.1f}% | "
            f"Throughput: {run_data['throughput_iterations_per_sec']} iter/s"
        )

    summary_suite = {
        "benchmark_type": "local_parallel",
        "iterations": iterations,
        "baseline_single_process_time_seconds": t_seq,
        "runs": results
    }

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(summary_suite, f, indent=2)

    logger.info(f"Rangkaian pengujian lokal selesai. Hasil disimpan di: {output_file}")
    return summary_suite


if __name__ == "__main__":
    mp.freeze_support()
    parser = argparse.ArgumentParser(description="Benchmark Local Parallel Processing")
    parser.add_argument("--iterations", type=int, default=10000, help="Jumlah iterasi")
    parser.add_argument("--processes", type=int, nargs="+", default=None, help="Daftar jumlah proses, contoh: 1 2 4 8")
    parser.add_argument("--output", type=str, default="results/benchmark_local_parallel.json", help="Output file")
    args = parser.parse_args()

    run_local_parallel_suite(process_counts=args.processes, iterations=args.iterations, output_file=args.output)

