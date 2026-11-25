"""
Visualisasi: performance_chart.py
Fokus: Membuat visualisasi grafik metrik performa komputasi terdistribusi:
       1. Speedup Aktual vs Ideal Linear Speedup.
       2. Efisiensi (%) vs Jumlah Worker (1, 2, 4, 6, 9 Worker).
       3. Throughput (Iterasi/Detik) vs Jumlah Worker.
       4. Perbandingan Execution Time (Sequential vs Local Parallel vs Distributed).
Bagian dari: System Optimization & Performance Evaluation (Anggota 3)
"""

import os
import json
import logging
import argparse
from typing import Dict, Any, Optional, List

import matplotlib
matplotlib.use("Agg")  # Mode headless
import matplotlib.pyplot as plt
import numpy as np

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(levelname)s] [PerformanceChart]: %(message)s")
logger = logging.getLogger("PerformanceChart")


def plot_speedup_and_efficiency(
    scalability_json_path: str = "results/scalability_results.json",
    output_png_path: str = "results/scalability_speedup_efficiency.png"
) -> Optional[str]:
    """
    Membuat grafik berdampingan (dual-panel subplot):
    - Panel Kiri: Speedup Aktual vs Speedup Linear Ideal
    - Panel Kanan: Efisiensi Komputasi (%) vs Jumlah Worker
    """
    if not os.path.exists(scalability_json_path):
        logger.error(f"File hasil skalabilitas {scalability_json_path} tidak ditemukan.")
        return None

    with open(scalability_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    experiments = data.get("experiments", [])
    if not experiments:
        logger.error("Data eksperimen skalabilitas kosong.")
        return None

    workers = [e["num_workers"] for e in experiments]
    times = [e["wall_clock_time_seconds"] for e in experiments]
    speedups = [
        e["speedup"] if e.get("speedup") is not None else (times[0] / e["wall_clock_time_seconds"] if e["wall_clock_time_seconds"] > 0 else 1.0)
        for e in experiments
    ]
    efficiencies = [
        (e["efficiency"] * 100) if e.get("efficiency") is not None else (s / w * 100)
        for e, s, w in zip(experiments, speedups, workers)
    ]

    plt.style.use("seaborn-v0_8-whitegrid" if "seaborn-v0_8-whitegrid" in plt.style.available else "default")
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6), dpi=300)

    # 1. Plot Speedup
    ax1.plot(workers, workers, "--", label="Ideal Linear Speedup ($S = P$)", color="#7f7f7f", linewidth=2)
    ax1.plot(workers, speedups, "o-", label="Aktual Speedup Terdistribusi", color="#1f77b4", linewidth=2.5, markersize=8)

    for w, s in zip(workers, speedups):
        ax1.annotate(f"{s:.2f}x", (w, s), textcoords="offset points", xytext=(0, 10), ha="center", fontweight="bold")

    ax1.set_title("Analisis Speedup Terdistribusi", fontsize=13, fontweight="bold", pad=12)
    ax1.set_xlabel("Jumlah Node Worker Fisik ($P$)", fontsize=11)
    ax1.set_ylabel("Speedup ($S = T_1 / T_p$)", fontsize=11)
    ax1.set_xticks(workers)
    ax1.grid(True, linestyle="--", alpha=0.6)
    ax1.legend(loc="upper left", frameon=True)

    # 2. Plot Efisiensi
    ax2.axhline(100, linestyle="--", color="#7f7f7f", label="Ideal Efisiensi (100%)", linewidth=2)
    ax2.plot(workers, efficiencies, "s-", label="Aktual Efisiensi", color="#2ca02c", linewidth=2.5, markersize=8)

    for w, eff in zip(workers, efficiencies):
        ax2.annotate(f"{eff:.1f}%", (w, eff), textcoords="offset points", xytext=(0, 10), ha="center", fontweight="bold")

    ax2.set_title("Analisis Efisiensi Komputasi Paralel", fontsize=13, fontweight="bold", pad=12)
    ax2.set_xlabel("Jumlah Node Worker Fisik ($P$)", fontsize=11)
    ax2.set_ylabel("Efisiensi ($E = S / P \\times 100\\%$)", fontsize=11)
    ax2.set_ylim(0, 115)
    ax2.set_xticks(workers)
    ax2.grid(True, linestyle="--", alpha=0.6)
    ax2.legend(loc="lower left", frameon=True)

    os.makedirs(os.path.dirname(output_png_path), exist_ok=True)
    plt.tight_layout()
    plt.savefig(output_png_path, dpi=300)
    plt.close()

    logger.info(f"Grafik Speedup & Efisiensi berhasil disimpan ke: {output_png_path}")
    return output_png_path


def plot_throughput_and_execution_time(
    scalability_json_path: str = "results/scalability_results.json",
    output_png_path: str = "results/scalability_throughput_time.png"
) -> Optional[str]:
    """
    Membuat grafik Throughput (Iterasi/Detik) dan Penurunan Waktu Eksekusi
    seiring bertambahnya jumlah Worker (1, 2, 4, 6, 9 node).
    """
    if not os.path.exists(scalability_json_path):
        logger.error(f"File {scalability_json_path} tidak ditemukan.")
        return None

    with open(scalability_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    experiments = data.get("experiments", [])
    workers = [e["num_workers"] for e in experiments]
    times = [e["wall_clock_time_seconds"] for e in experiments]
    throughputs = [e["throughput_iterations_per_sec"] for e in experiments]

    plt.style.use("seaborn-v0_8-whitegrid" if "seaborn-v0_8-whitegrid" in plt.style.available else "default")
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6), dpi=300)

    # Panel 1: Throughput
    ax1.plot(workers, throughputs, "D-", color="#d62728", linewidth=2.5, markersize=8, label="Throughput (iter/s)")
    for w, tp in zip(workers, throughputs):
        ax1.annotate(f"{tp:,.0f}", (w, tp), textcoords="offset points", xytext=(0, 10), ha="center", fontweight="bold")

    ax1.set_title("Throughput Sistem Terdistribusi", fontsize=13, fontweight="bold", pad=12)
    ax1.set_xlabel("Jumlah Node Worker Fisik", fontsize=11)
    ax1.set_ylabel("Throughput (Iterasi / Detik)", fontsize=11)
    ax1.set_xticks(workers)
    ax1.grid(True, linestyle="--", alpha=0.6)
    ax1.legend(loc="upper left", frameon=True)

    # Panel 2: Execution Time
    ax2.bar([str(w) for w in workers], times, color="#17becf", edgecolor="#0e717b", width=0.55, label="Wall-Clock Time")
    for idx, (w, t) in enumerate(zip(workers, times)):
        ax2.annotate(f"{t:.2f}s", (idx, t), textcoords="offset points", xytext=(0, 5), ha="center", fontweight="bold")

    ax2.set_title("Penurunan Waktu Eksekusi (Execution Time)", fontsize=13, fontweight="bold", pad=12)
    ax2.set_xlabel("Jumlah Node Worker Fisik", fontsize=11)
    ax2.set_ylabel("Waktu Eksekusi (Detik)", fontsize=11)
    ax2.grid(True, linestyle="--", alpha=0.6)
    ax2.legend(loc="upper right", frameon=True)

    os.makedirs(os.path.dirname(output_png_path), exist_ok=True)
    plt.tight_layout()
    plt.savefig(output_png_path, dpi=300)
    plt.close()

    logger.info(f"Grafik Throughput & Execution Time berhasil disimpan ke: {output_png_path}")
    return output_png_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Visualisasi Metrik Performa Sistem Terdistribusi")
    parser.add_argument("--input", type=str, default="results/scalability_results.json", help="Path JSON skalabilitas")
    parser.add_argument("--out-speedup", type=str, default="results/scalability_speedup_efficiency.png")
    parser.add_argument("--out-throughput", type=str, default="results/scalability_throughput_time.png")
    args = parser.parse_args()

    plot_speedup_and_efficiency(args.input, args.out_speedup)
    plot_throughput_and_execution_time(args.input, args.out_throughput)
