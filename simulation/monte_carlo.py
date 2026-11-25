"""Mesin Monte Carlo untuk menjalankan satu chunk task simulasi SIR."""

from typing import Any, Dict

import numpy as np

from simulation.simulation_config import normalize_config
from simulation.sir_model import run_sir_once


def run_chunk(config: Any, iterations: int, seed: int) -> Dict[str, Any]:
    """Menjalankan banyak simulasi SIR dan mengembalikan rata-rata hasil chunk."""
    if iterations <= 0:
        raise ValueError("iterations harus lebih besar dari 0.")

    cfg = normalize_config(config)
    time_steps = int(cfg.time_steps)

    s_acc = np.zeros(time_steps + 1, dtype=np.float64)
    i_acc = np.zeros(time_steps + 1, dtype=np.float64)
    r_acc = np.zeros(time_steps + 1, dtype=np.float64)

    peak_acc = 0.0
    peak_day_acc = 0.0
    total_infected_acc = 0.0

    root_rng = np.random.default_rng(seed)
    run_seeds = root_rng.integers(0, np.iinfo(np.uint32).max, size=iterations, dtype=np.uint32)

    for run_seed in run_seeds:
        one_run = run_sir_once(cfg, int(run_seed))
        s_acc += one_run["s"]
        i_acc += one_run["i"]
        r_acc += one_run["r"]
        peak_acc += one_run["peak_infected"]
        peak_day_acc += one_run["peak_day"]
        total_infected_acc += one_run["total_infected"]

    inv_n = 1.0 / float(iterations)

    return {
        "simulation_summary": {
            "peak_infected_mean": round(peak_acc * inv_n, 2),
            "peak_day_mean": round(peak_day_acc * inv_n, 2),
            "total_infected_mean": round(total_infected_acc * inv_n, 2),
        },
        "trajectories": {
            "mean_s": np.round(s_acc * inv_n, 2).tolist(),
            "mean_i": np.round(i_acc * inv_n, 2).tolist(),
            "mean_r": np.round(r_acc * inv_n, 2).tolist(),
        },
    }
