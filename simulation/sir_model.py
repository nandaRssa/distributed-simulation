"""Simulasi satu lintasan model SIR stokastik chain-binomial berbasis NumPy."""

from typing import Any, Dict

import numpy as np

from simulation.simulation_config import normalize_config


def run_sir_once(config: Any, seed: int) -> Dict[str, Any]:
    """Menjalankan satu realisasi SIR stokastik dan mengembalikan metrik lintasan."""
    cfg = normalize_config(config)
    rng = np.random.default_rng(seed)

    population = int(cfg.population)
    s = int(population - cfg.initial_infected - cfg.initial_recovered)
    i = int(cfg.initial_infected)
    r = int(cfg.initial_recovered)
    time_steps = int(cfg.time_steps)

    s_traj = np.zeros(time_steps + 1, dtype=np.float64)
    i_traj = np.zeros(time_steps + 1, dtype=np.float64)
    r_traj = np.zeros(time_steps + 1, dtype=np.float64)

    peak_infected = i
    peak_day = 0

    for t in range(time_steps + 1):
        s_traj[t] = s
        i_traj[t] = i
        r_traj[t] = r

        if i > peak_infected:
            peak_infected = i
            peak_day = t

        if t == time_steps:
            break

        p_inf = float(1.0 - np.exp(-cfg.beta * i / population))
        p_rec = float(1.0 - np.exp(-cfg.gamma))
        p_inf = min(max(p_inf, 0.0), 1.0)
        p_rec = min(max(p_rec, 0.0), 1.0)

        new_inf = int(rng.binomial(s, p_inf))
        new_rec = int(rng.binomial(i, p_rec))

        s -= new_inf
        i += (new_inf - new_rec)
        r += new_rec

    total_infected = float(population - s)

    return {
        "s": s_traj,
        "i": i_traj,
        "r": r_traj,
        "peak_infected": float(peak_infected),
        "peak_day": float(peak_day),
        "total_infected": total_infected,
    }
