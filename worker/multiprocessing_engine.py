"""Engine multiprocessing untuk mengeksekusi satu chunk task simulasi SIR."""

import multiprocessing as mp
from typing import Any, Dict, List, Tuple

import numpy as np

from simulation.monte_carlo import run_chunk


def _run_subchunk_worker(args: Tuple[Dict[str, Any], int, int]) -> Dict[str, Any]:
    """Fungsi worker level modul agar kompatibel dengan mode spawn di Windows."""
    config, iterations, seed = args
    return run_chunk(config=config, iterations=iterations, seed=seed)


def _partition_iterations(total_iterations: int, partitions: int) -> List[int]:
    """Membagi total iterasi ke beberapa sub-chunk secara merata."""
    if total_iterations <= 0:
        raise ValueError("total_iterations harus > 0")
    if partitions <= 0:
        raise ValueError("partitions harus > 0")

    partitions = min(total_iterations, partitions)
    base = total_iterations // partitions
    remainder = total_iterations % partitions

    result: List[int] = []
    for idx in range(partitions):
        result.append(base + (1 if idx < remainder else 0))
    return result


class MultiprocessingSimulationEngine:
    """Menjalankan simulasi chunk dengan proses paralel dan penggabungan weighted average."""

    def __init__(self, processes: int):
        if processes <= 0:
            raise ValueError("processes harus > 0")
        self.processes = processes
        self._pool: mp.pool.Pool | None = None
        self._ctx = mp.get_context("spawn")

    def start(self) -> None:
        """Membuat process pool satu kali selama siklus hidup worker."""
        if self._pool is None:
            self._pool = self._ctx.Pool(processes=self.processes)

    def stop(self) -> None:
        """Menutup pool secara bersih saat worker shutdown."""
        if self._pool is not None:
            self._pool.close()
            self._pool.join()
            self._pool = None

    def run_task(self, config: Dict[str, Any], iterations: int, seed: int) -> Dict[str, Any]:
        """Menjalankan satu task menggunakan sub-chunk paralel."""
        if iterations <= 0:
            raise ValueError("iterations harus > 0")

        chunk_sizes = _partition_iterations(iterations, self.processes)
        if len(chunk_sizes) == 1:
            return run_chunk(config=config, iterations=iterations, seed=seed)

        seed_sequence = np.random.SeedSequence(seed)
        child_sequences = seed_sequence.spawn(len(chunk_sizes))
        child_seeds = [int(child.generate_state(1, dtype=np.uint32)[0]) for child in child_sequences]

        work_items = [
            (config, chunk_iter, chunk_seed)
            for chunk_iter, chunk_seed in zip(chunk_sizes, child_seeds)
        ]

        if self._pool is None:
            self.start()

        assert self._pool is not None
        partial_results = self._pool.map(_run_subchunk_worker, work_items)
        return self._merge_weighted(partial_results, chunk_sizes)

    @staticmethod
    def _merge_weighted(partial_results: List[Dict[str, Any]], chunk_sizes: List[int]) -> Dict[str, Any]:
        """Menggabungkan hasil sub-chunk dengan bobot jumlah iterasi."""
        total_iterations = float(sum(chunk_sizes))
        if total_iterations <= 0:
            raise ValueError("total iterasi gabungan tidak valid")

        first = partial_results[0]
        len_steps = len(first["trajectories"]["mean_s"])

        mean_s = np.zeros(len_steps, dtype=np.float64)
        mean_i = np.zeros(len_steps, dtype=np.float64)
        mean_r = np.zeros(len_steps, dtype=np.float64)

        peak_infected_mean = 0.0
        peak_day_mean = 0.0
        total_infected_mean = 0.0

        for result, chunk_iterations in zip(partial_results, chunk_sizes):
            weight = float(chunk_iterations) / total_iterations
            summary = result["simulation_summary"]
            traj = result["trajectories"]

            peak_infected_mean += weight * float(summary["peak_infected_mean"])
            peak_day_mean += weight * float(summary["peak_day_mean"])
            total_infected_mean += weight * float(summary["total_infected_mean"])

            mean_s += weight * np.array(traj["mean_s"], dtype=np.float64)
            mean_i += weight * np.array(traj["mean_i"], dtype=np.float64)
            mean_r += weight * np.array(traj["mean_r"], dtype=np.float64)

        return {
            "simulation_summary": {
                "peak_infected_mean": round(float(peak_infected_mean), 2),
                "peak_day_mean": round(float(peak_day_mean), 2),
                "total_infected_mean": round(float(total_infected_mean), 2),
            },
            "trajectories": {
                "mean_s": np.round(mean_s, 2).tolist(),
                "mean_i": np.round(mean_i, 2).tolist(),
                "mean_r": np.round(mean_r, 2).tolist(),
            },
        }


if __name__ == "__main__":
    mp.freeze_support()
