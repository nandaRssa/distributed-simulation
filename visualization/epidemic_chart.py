"""
Visualisasi: epidemic_chart.py
Fokus: Membuat visualisasi kurva epidemiologi model SIR S(t), I(t), R(t)
       berdasarkan hasil agregasi simulasi Monte Carlo terdistribusi.
Bagian dari: System Optimization & Performance Evaluation (Anggota 3)
"""

import os
import json
import logging
import argparse
from typing import Dict, Any, Optional

import matplotlib
matplotlib.use("Agg")  # Mode headless tanpa GUI window
import matplotlib.pyplot as plt
import numpy as np

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(levelname)s] [EpidemicChart]: %(message)s")
logger = logging.getLogger("EpidemicChart")


def plot_epidemic_curves(
    result_json_path: str = "results/simulation_result.json",
    output_png_path: str = "results/epidemic_curve.png",
    title_suffix: str = ""
) -> Optional[str]:
    """
    Membaca hasil agregasi simulasi JSON dan membuat grafik kurva S(t), I(t), R(t).
    Grafik disimpan dalam resolusi tinggi (300 DPI) untuk kebutuhan BAB IV laporan.
    """
    if not os.path.exists(result_json_path):
        logger.error(f"File hasil simulasi {result_json_path} tidak ditemukan.")
        return None

    with open(result_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    trajectories = data.get("trajectories", {})
    summary = data.get("summary", {})

    s_curve = np.array(trajectories.get("mean_s", []))
    i_curve = np.array(trajectories.get("mean_i", []))
    r_curve = np.array(trajectories.get("mean_r", []))

    if len(s_curve) == 0:
        logger.error("Data lintasan kurva trajektori kosong.")
        return None

    days = np.arange(len(s_curve))
    peak_infected = summary.get("peak_infected_mean", float(np.max(i_curve)))
    peak_day = int(summary.get("peak_day_mean", int(np.argmax(i_curve))))
    total_iterations = data.get("total_iterations", 0)

    # Plot styling
    plt.style.use("seaborn-v0_8-whitegrid" if "seaborn-v0_8-whitegrid" in plt.style.available else "default")
    fig, ax = plt.subplots(figsize=(10, 6), dpi=300)

    ax.plot(days, s_curve, label="Susceptible (Rentan)", color="#1f77b4", linewidth=2.5)
    ax.plot(days, i_curve, label="Infected (Terinfeksi)", color="#d62728", linewidth=2.5)
    ax.plot(days, r_curve, label="Recovered (Sembuh/Kebal)", color="#2ca02c", linewidth=2.5)

    # Tandai titik puncak infeksi
    ax.scatter([peak_day], [peak_infected], color="#d62728", s=100, zorder=5, edgecolors="black", linewidth=1.5)
    ax.annotate(
        f"Puncak Infeksi\nHari ke-{peak_day}: {peak_infected:,.0f} jiwa",
        xy=(peak_day, peak_infected),
        xytext=(peak_day + len(days) * 0.05, peak_infected * 0.95),
        arrowprops=dict(facecolor="black", shrink=0.08, width=1.5, headwidth=8),
        fontsize=10,
        fontweight="bold",
        bbox=dict(boxstyle="round,pad=0.5", facecolor="#fff2f2", edgecolor="#d62728", alpha=0.9)
    )

    ax.set_title(
        f"Dinamika Penyebaran Epidemi Model SIR\n(Monte Carlo Simulation - {total_iterations:,} Iterasi) {title_suffix}",
        fontsize=14,
        fontweight="bold",
        pad=15
    )
    ax.set_xlabel("Waktu Pengamatan (Hari)", fontsize=12, labelpad=10)
    ax.set_ylabel("Populasi Individu", fontsize=12, labelpad=10)
    ax.grid(True, linestyle="--", alpha=0.6)
    ax.legend(loc="center right", fontsize=11, frameon=True, shadow=True)

    os.makedirs(os.path.dirname(output_png_path), exist_ok=True)
    plt.tight_layout()
    plt.savefig(output_png_path, dpi=300)
    plt.close()

    logger.info(f"Grafik epidemiologi berhasil disimpan ke: {output_png_path}")
    return output_png_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Visualisasi Kurva Epidemiologi Model SIR")
    parser.add_argument("--input", type=str, default="results/simulation_result.json", help="Path input JSON simulasi")
    parser.add_argument("--output", type=str, default="results/epidemic_curve.png", help="Path output file PNG")
    args = parser.parse_args()

    plot_epidemic_curves(result_json_path=args.input, output_png_path=args.output)

