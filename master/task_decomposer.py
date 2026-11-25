"""
Module: task_decomposer.py
Fokus: Dekomposisi iterasi simulasi Monte Carlo SIR menjadi sub-task independen.
Bagian dari: Master Controller (Anggota 1)
"""

import time
import uuid
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, List, Optional


@dataclass
class SimulationConfig:
    """Parameter konfigurasi model SIR untuk simulasi epidemi."""
    population: int = 10000
    initial_infected: int = 10
    initial_recovered: int = 0
    beta: float = 0.3      # Laju penularan (infection rate)
    gamma: float = 0.1     # Laju kesembuhan (recovery rate)
    time_steps: int = 100  # Periode hari pengamatan

    def validate(self) -> None:
        """Memvalidasi parameter fisik simulasi epidemi."""
        if self.population <= 0:
            raise ValueError("Population harus lebih besar dari 0.")
        if self.initial_infected <= 0 or self.initial_infected > self.population:
            raise ValueError("Initial infected harus > 0 dan <= total populasi.")
        if self.initial_recovered < 0 or (self.initial_infected + self.initial_recovered) > self.population:
            raise ValueError("Jumlah initial infected + recovered tidak boleh melebihi populasi.")
        if not (0.0 <= self.beta <= 1.0):
            raise ValueError("Beta (infection rate) harus berada pada rentang [0.0, 1.0].")
        if not (0.0 <= self.gamma <= 1.0):
            raise ValueError("Gamma (recovery rate) harus berada pada rentang [0.0, 1.0].")
        if self.time_steps <= 0:
            raise ValueError("Time steps harus lebih besar dari 0.")



@dataclass
class Task:
    """Representasi data sebuah task simulasi independen."""
    task_id: str
    iterations: int
    simulation_config: Dict[str, Any]
    random_seed: int
    created_at: float = field(default_factory=time.time)
    assigned_worker_id: Optional[str] = None
    dispatched_at: Optional[float] = None
    completed_at: Optional[float] = None
    retry_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Konversi objek task ke dictionary serializable untuk pengiriman JSON."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Task":
        """Rekonstruksi objek Task dari dictionary."""
        return cls(**data)


class TaskDecomposer:
    """
    Bertanggung jawab memecah total iterasi Monte Carlo menjadi sekumpulan task
    yang independen secara komputasi dan statistik (menggunakan PRNG seed unik).
    """

    def __init__(self, base_seed: int = 42):
        self.base_seed = base_seed

    def decompose(
        self,
        total_iterations: int,
        chunk_size: int,
        config: SimulationConfig,
        prefix: str = "TASK"
    ) -> List[Task]:
        """
        Membagi total_iterations menjadi list of Task dengan ukuran chunk_size.
        
        Algoritma Pembagian Sisa (Remainder Handling):
        - Jika total_iterations tidak habis dibagi chunk_size, sisa iterasi (remainder)
          didistribusikan satu per satu ke task awal, atau chunk sisa dijadikan task
          tersendiri bergantung pada ukuran chunk.
        - Di sini kita menggunakan pendekatan adil:
          Base tasks = total_iterations // chunk_size.
          Jika ada sisa, sisa didistribusikan merata ke task-task awal (+1 per task),
          atau jika total_iterations < chunk_size, dibuat tepat 1 task berisi total_iterations.
        
        Menjamin 100% persamaan matematika:
        sum(task.iterations for task in tasks) == total_iterations
        """
        if total_iterations <= 0:
            raise ValueError("total_iterations harus lebih besar dari 0.")
        if chunk_size <= 0:
            raise ValueError("chunk_size harus lebih besar dari 0.")

        config.validate()
        config_dict = asdict(config)

        # Kasus jika total_iterations lebih kecil dari ukuran chunk
        if total_iterations <= chunk_size:
            seed = self._generate_seed(0)
            task_id = f"{prefix}-0001"
            return [
                Task(
                    task_id=task_id,
                    iterations=total_iterations,
                    simulation_config=config_dict,
                    random_seed=seed
                )
            ]

        num_tasks = total_iterations // chunk_size
        remainder = total_iterations % chunk_size

        tasks: List[Task] = []
        iteration_counts: List[int] = []

        # Alokasi dasar
        for _ in range(num_tasks):
            iteration_counts.append(chunk_size)

        # Distribusi sisa (remainder) satu per satu ke task yang ada
        for i in range(remainder):
            iteration_counts[i] += 1

        # Pembuatan Task Metadata
        digits = max(4, len(str(len(iteration_counts))))
        for idx, iters in enumerate(iteration_counts, start=1):
            task_id = f"{prefix}-{str(idx).zfill(digits)}"
            seed = self._generate_seed(idx)
            
            task = Task(
                task_id=task_id,
                iterations=iters,
                simulation_config=config_dict,
                random_seed=seed
            )
            tasks.append(task)

        # Verifikasi matematis integritas total iterasi
        allocated_iterations = sum(t.iterations for t in tasks)
        assert allocated_iterations == total_iterations, (
            f"Ketidaksesuaian partisi! Diharapkan {total_iterations}, tetapi teralokasi {allocated_iterations}"
        )

        return tasks

    def _generate_seed(self, index: int) -> int:
        """
        Menghasilkan seed pseudo-random unik untuk setiap task menggunakan formula
        stride bilangan prima besar untuk meminimalkan korelasi seed antar worker.
        """
        PRIME_STRIDE = 10007
        return (self.base_seed + (index * PRIME_STRIDE)) & 0x7FFFFFFF
