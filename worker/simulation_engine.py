"""Lapisan eksekusi task simulasi untuk Worker Node."""

import time
from typing import Any, Dict, Optional

from worker.multiprocessing_engine import MultiprocessingSimulationEngine

_ENGINE: Optional[MultiprocessingSimulationEngine] = None


def initialize_engine(processes: int) -> None:
    """Inisialisasi process pool satu kali di awal worker."""
    global _ENGINE
    if _ENGINE is None or _ENGINE.processes != processes:
        if _ENGINE is not None:
            _ENGINE.stop()
        _ENGINE = MultiprocessingSimulationEngine(processes=processes)
        _ENGINE.start()


def shutdown_engine() -> None:
    """Menutup process pool secara bersih saat worker berhenti."""
    global _ENGINE
    if _ENGINE is not None:
        _ENGINE.stop()
        _ENGINE = None


def execute_task(payload: Dict[str, Any], processes: int) -> Dict[str, Any]:
    """Eksekusi task sesuai payload protokol dan kembalikan result lengkap."""
    task_id = payload.get("task_id")
    worker_id = payload.get("worker_id")
    iterations = int(payload.get("iterations", 0))
    config = payload.get("simulation_config", {})
    random_seed = int(payload.get("random_seed", 42))

    if not task_id:
        raise ValueError("task_id wajib ada pada payload task.")
    if not worker_id:
        raise ValueError("worker_id wajib tersedia untuk penyusunan payload result.")
    if iterations <= 0:
        raise ValueError("iterations harus > 0.")

    initialize_engine(processes)
    assert _ENGINE is not None

    t_start = time.time()
    sim_data = _ENGINE.run_task(config=config, iterations=iterations, seed=random_seed)
    t_elapsed = time.time() - t_start

    return {
        "task_id": task_id,
        "worker_id": worker_id,
        "execution_time_seconds": round(t_elapsed, 4),
        "iterations_completed": iterations,
        "simulation_summary": sim_data["simulation_summary"],
        "trajectories": sim_data["trajectories"],
        "cores_used": processes,
    }
