"""
Benchmark: scalability_test.py
Fokus: Pengujian skalabilitas Master-Worker terdistribusi dengan variasi
       1, 2, 4, 6, dan 9 Worker (sesuai ketentuan wajib spesifikasi UAS).
       Menghasilkan tabel komparasi Execution Time, Speedup, Efficiency, Throughput,
       dan utilisasi sumber daya dalam format JSON dan CSV untuk BAB IV laporan.
Bagian dari: System Optimization & Performance Evaluation (Anggota 3)
"""

import sys
import os
import time
import json
import csv
import socket
import logging
import argparse
import threading
from typing import Dict, Any, List, Optional

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from master.master import MasterController
from worker.worker import WorkerNode
from benchmark.sequential import run_sequential_benchmark

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [ScalabilitySuite]: %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("ScalabilitySuite")


def _find_free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def run_scalability_point(
    num_workers: int,
    total_iterations: int = 18000,
    chunk_size: int = 1000,
    cores_per_worker: int = 2,
    sequential_baseline_time: Optional[float] = None
) -> Dict[str, Any]:
    """Menjalankan satu titik uji skalabilitas dengan N worker."""
    port = _find_free_port()
    output_temp = f"results/temp_scalability_{num_workers}w.json"

    logger.info(
        f"=== Memulai Uji Skalabilitas: {num_workers} Worker "
        f"({total_iterations} Iterasi, Chunk: {chunk_size}) ==="
    )

    controller = MasterController(
        host="127.0.0.1",
        port=port,
        total_iterations=total_iterations,
        chunk_size=chunk_size,
        min_workers=num_workers,
        output_file=output_temp,
        enable_fault_tolerance=True
    )

    master_th = threading.Thread(target=controller.start, args=(60.0,), name=f"Master-{num_workers}W")
    master_th.start()
    time.sleep(0.15)

    workers: List[WorkerNode] = []
    worker_threads: List[threading.Thread] = []

    for i in range(1, num_workers + 1):
        w = WorkerNode(
            master_host="127.0.0.1",
            master_port=port,
            worker_id=f"PHYSICAL-NODE-{i:02d}",
            cores=cores_per_worker,
            processes=cores_per_worker
        )
        workers.append(w)
        th = threading.Thread(target=w.start, name=f"WorkerThread-{i:02d}")
        worker_threads.append(th)
        th.start()

    master_th.join(timeout=75.0)
    for th in worker_threads:
        th.join(timeout=5.0)

    # Ambil metrik performa
    metrics = controller.performance_monitor.compute_evaluation_metrics(
        total_iterations=total_iterations,
        sequential_baseline_time=sequential_baseline_time,
        num_processing_units=num_workers
    )

    # Bersihkan file temp
    if os.path.exists(output_temp):
        try:
            os.remove(output_temp)
        except Exception:
            pass

    return {
        "num_workers": num_workers,
        "total_iterations": total_iterations,
        "wall_clock_time_seconds": metrics["wall_clock_time_seconds"],
        "throughput_iterations_per_sec": metrics["throughput_iterations_per_sec"],
        "speedup": metrics["speedup"],
        "efficiency": metrics["efficiency"],
        "avg_cpu_percent": metrics["cpu_utilization"]["avg_percent"],
        "peak_cpu_percent": metrics["cpu_utilization"]["peak_percent"],
        "avg_memory_mb": metrics["memory_utilization"]["avg_mb"],
        "peak_memory_mb": metrics["memory_utilization"]["peak_mb"]
    }


def run_full_scalability_experiment(
    worker_counts: List[int] = None,
    total_iterations: int = 18000,
    chunk_size: int = 1000,
    run_baseline_first: bool = True,
    output_json: str = "results/scalability_results.json",
    output_csv: str = "results/scalability_results.csv"
) -> Dict[str, Any]:
    """
    Menjalankan pengujian skalabilitas lengkap untuk 1, 2, 4, 6, dan 9 Worker.
    Menghitung Speedup dan Efisiensi terhadap baseline sequential 1 core.
    """
    if worker_counts is None:
        worker_counts = [1, 2, 4, 6, 9]

    logger.info("=" * 70)
    logger.info("MEMULAI PENGUJIAN SKALABILITAS SISTEM TERDISTRIBUSI (UAS KOMPUTASI PARALEL)")
    logger.info(f"Target Konfigurasi Node: {worker_counts} Worker")
    logger.info(f"Total Iterasi         : {total_iterations}")
    logger.info("=" * 70)

    # 1. Jalankan atau hitung baseline sequential
    seq_time = None
    if run_baseline_first:
        logger.info("Menjalankan Sequential Baseline untuk acuan Speedup...")
        seq_res = run_sequential_benchmark(
            iterations=total_iterations,
            output_file="results/benchmark_sequential.json"
        )
        seq_time = seq_res["wall_clock_time_seconds"]
        logger.info(f"Baseline Sequential Time: {seq_time:.3f} s")

    # 2. Uji setiap variasi worker
    results: List[Dict[str, Any]] = []
    for count in worker_counts:
        res = run_scalability_point(
            num_workers=count,
            total_iterations=total_iterations,
            chunk_size=chunk_size,
            cores_per_worker=2,
            sequential_baseline_time=seq_time
        )
        results.append(res)
        logger.info(
            f"[HASIL {count} WORKER] Waktu: {res['wall_clock_time_seconds']}s | "
            f"Speedup: {res['speedup']}x | Efisiensi: {(res['efficiency'] or 0)*100:.1f}% | "
            f"Throughput: {res['throughput_iterations_per_sec']} iter/s"
        )

    full_report = {
        "title": "Scalability Evaluation Report - Distributed Monte Carlo SIR Simulation",
        "total_iterations": total_iterations,
        "chunk_size": chunk_size,
        "sequential_baseline_time_seconds": seq_time,
        "experiments": results
    }

    # 3. Simpan ke JSON
    os.makedirs(os.path.dirname(output_json), exist_ok=True)
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(full_report, f, indent=2)

    # 4. Simpan ke CSV untuk tabel BAB IV Laporan
    fieldnames = [
        "num_workers",
        "total_iterations",
        "wall_clock_time_seconds",
        "throughput_iterations_per_sec",
        "speedup",
        "efficiency",
        "avg_cpu_percent",
        "peak_cpu_percent",
        "avg_memory_mb",
        "peak_memory_mb"
    ]
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            writer.writerow(r)

    logger.info("=" * 70)
    logger.info("PENGUJIAN SKALABILITAS SELESAI")
    logger.info(f"File Laporan JSON : {output_json}")
    logger.info(f"File Tabel CSV    : {output_csv}")
    logger.info("=" * 70)

    return full_report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scalability Benchmark Suite (1, 2, 4, 6, 9 Workers)")
    parser.add_argument("--iterations", type=int, default=18000, help="Total iterasi Monte Carlo")
    parser.add_argument("--chunk-size", type=int, default=1000, help="Iterasi per task")
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4, 6, 9], help="Variasi jumlah worker")
    parser.add_argument("--skip-baseline", action="store_true", help="Lewati sequential baseline")
    parser.add_argument("--output-json", type=str, default="results/scalability_results.json", help="Path JSON output")
    parser.add_argument("--output-csv", type=str, default="results/scalability_results.csv", help="Path CSV output")
    args = parser.parse_args()

    run_full_scalability_experiment(
        worker_counts=args.workers,
        total_iterations=args.iterations,
        chunk_size=args.chunk_size,
        run_baseline_first=not args.skip_baseline,
        output_json=args.output_json,
        output_csv=args.output_csv
    )

