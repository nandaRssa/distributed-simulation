"""
Benchmark: distributed.py
Fokus: Pengujian komputasi terdistribusi Master-Worker pada jaringan.
       Mencatat waktu eksekusi total, throughput agregat, kontribusi task per Worker,
       utilisasi CPU dan RAM, serta perbandingannya dengan baseline serial.
Bagian dari: System Optimization & Performance Evaluation (Anggota 3)
"""

import sys
import os
import time
import json
import logging
import argparse
from typing import Dict, Any, Optional

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from master.master import MasterController
from simulation.simulation_config import SimulationConfig

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [DistributedBenchmark]: %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("DistributedBenchmark")


def run_distributed_benchmark(
    host: str = "0.0.0.0",
    port: int = 5000,
    iterations: int = 20000,
    chunk_size: int = 1000,
    min_workers: int = 1,
    sequential_baseline_time: Optional[float] = None,
    output_file: str = "results/benchmark_distributed.json"
) -> Optional[Dict[str, Any]]:
    """
    Menjalankan pengujian performa simulasi terdistribusi menggunakan MasterController.
    Master akan menunggu min_workers terhubung, mendistribusikan task secara dinamis,
    mengagregasi hasil, dan menghitung metrik evaluasi (Speedup, Efisiensi, Throughput).
    """
    logger.info(
        f"Memulai Distributed Benchmark: {iterations} iterasi (Chunk: {chunk_size}, Min Workers: {min_workers})..."
    )

    controller = MasterController(
        host=host,
        port=port,
        total_iterations=iterations,
        chunk_size=chunk_size,
        min_workers=min_workers,
        output_file=output_file,
        enable_fault_tolerance=True
    )

    aggregated_result = controller.start(wait_timeout=300.0)
    if not aggregated_result:
        logger.error("Distributed Benchmark gagal diselesaikan.")
        return None

    # Hitung metrik evaluasi
    eval_metrics = controller.performance_monitor.compute_evaluation_metrics(
        total_iterations=iterations,
        sequential_baseline_time=sequential_baseline_time,
        num_processing_units=len(controller.worker_manager.get_all_workers())
    )

    benchmark_summary = {
        "benchmark_type": "distributed",
        "total_iterations": iterations,
        "chunk_size": chunk_size,
        "connected_workers_count": len(controller.worker_manager.get_all_workers()),
        "per_worker_contributions": aggregated_result.per_worker_contributions,
        "evaluation_metrics": eval_metrics,
        "simulation_summary": aggregated_result.summary
    }

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(benchmark_summary, f, indent=2)

    logger.info(f"Hasil Distributed Benchmark disimpan ke: {output_file}")
    return benchmark_summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed Master-Worker Benchmark")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="IP listen master (0.0.0.0)")
    parser.add_argument("--port", type=int, default=5000, help="Port TCP master")
    parser.add_argument("--iterations", type=int, default=20000, help="Total iterasi Monte Carlo")
    parser.add_argument("--chunk-size", type=int, default=1000, help="Iterasi per task")
    parser.add_argument("--min-workers", type=int, default=1, help="Jumlah worker minimal yang ditunggu")
    parser.add_argument("--baseline-time", type=float, default=None, help="Waktu sequential baseline (detik)")
    parser.add_argument("--output", type=str, default="results/benchmark_distributed.json", help="Output file JSON")
    args = parser.parse_args()

    run_distributed_benchmark(
        host=args.host,
        port=args.port,
        iterations=args.iterations,
        chunk_size=args.chunk_size,
        min_workers=args.min_workers,
        sequential_baseline_time=args.baseline_time,
        output_file=args.output
    )

