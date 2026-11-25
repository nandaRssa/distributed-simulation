"""
Benchmark: sequential.py
Fokus: Pengujian baseline komputasi sequential (single core / satu proses)
       sebagai acuan dasar perhitungan Speedup dan Efficiency (T_seq).
Bagian dari: System Optimization & Performance Evaluation (Anggota 3)
"""

import sys
import os
import time
import json
import logging
import argparse
from typing import Dict, Any

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from simulation.simulation_config import SimulationConfig, normalize_config
from simulation.monte_carlo import run_chunk
from master.performance_monitor import PerformanceMonitor

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [SequentialBenchmark]: %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("SequentialBenchmark")


def run_sequential_benchmark(
    iterations: int = 10000,
    config: Any = None,
    seed: int = 42,
    output_file: str = "results/benchmark_sequential.json"
) -> Dict[str, Any]:
    """
    Menjalankan simulasi Monte Carlo SIR secara serial murni (1 process/thread).
    Mengukur wall-clock time, throughput, CPU utilization, dan memory utilization.
    """
    cfg = normalize_config(config or SimulationConfig())
    logger.info(f"Memulai Benchmark Sequential: {iterations} iterasi (Single Process)...")

    monitor = PerformanceMonitor(sample_interval_seconds=0.2)
    monitor.start()

    t_start = time.time()
    result_data = run_chunk(config=cfg, iterations=iterations, seed=seed)
    t_end = time.time()

    monitor.stop()

    wall_clock = t_end - t_start
    throughput = (iterations / wall_clock) if wall_clock > 0 else 0.0
    resource_stats = monitor.get_resource_summary()

    benchmark_result = {
        "benchmark_type": "sequential",
        "iterations": iterations,
        "processes": 1,
        "wall_clock_time_seconds": round(wall_clock, 4),
        "throughput_iterations_per_sec": round(throughput, 2),
        "cpu_utilization": {
            "avg_percent": resource_stats["avg_cpu_percent"],
            "peak_percent": resource_stats["peak_cpu_percent"]
        },
        "memory_utilization": {
            "avg_mb": resource_stats["avg_memory_mb"],
            "peak_mb": resource_stats["peak_memory_mb"]
        },
        "simulation_summary": result_data["simulation_summary"],
        "trajectories": result_data["trajectories"]
    }

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(benchmark_result, f, indent=2)

    logger.info("=" * 60)
    logger.info("HASIL BENCHMARK SEQUENTIAL (BASELINE 1 PROCESS)")
    logger.info("=" * 60)
    logger.info(f"Total Iterasi         : {iterations}")
    logger.info(f"Execution Time (T_seq): {wall_clock:.4f} detik")
    logger.info(f"Throughput            : {throughput:.2f} iterasi/detik")
    logger.info(f"Rata-rata CPU         : {resource_stats['avg_cpu_percent']:.1f}%")
    logger.info(f"Rata-rata RAM         : {resource_stats['avg_memory_mb']:.1f} MB")
    logger.info(f"File Hasil Disimpan   : {output_file}")
    logger.info("=" * 60)

    return benchmark_result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark Sequential (Baseline 1 Process)")
    parser.add_argument("--iterations", type=int, default=10000, help="Jumlah iterasi Monte Carlo")
    parser.add_argument("--output", type=str, default="results/benchmark_sequential.json", help="Path file output JSON")
    args = parser.parse_args()

    run_sequential_benchmark(iterations=args.iterations, output_file=args.output)

