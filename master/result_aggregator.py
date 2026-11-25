"""
Module: result_aggregator.py
Fokus: Pengumpulan, validasi integritas, pencegahan duplikasi, dan penggabungan
       (agregasi bobot iterasi) hasil simulasi Monte Carlo SIR dari seluruh Worker.
Bagian dari: Master Controller (Anggota 1)
"""

import json
import os
import time
import threading
import logging
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Any, Set

logger = logging.getLogger("ResultAggregator")


@dataclass
class TaskResult:
    """Representasi data hasil eksekusi dari satu paket task simulasi."""
    task_id: str
    worker_id: str
    execution_time_seconds: float
    iterations_completed: int
    simulation_summary: Dict[str, float]
    trajectories: Dict[str, List[float]]  # mean_s, mean_i, mean_r
    received_at: float = field(default_factory=time.time)


@dataclass
class AggregatedSimulationResult:
    """Hasil akhir gabungan seluruh eksperimen Monte Carlo SIR terdistribusi."""
    total_tasks_completed: int
    total_iterations: int
    total_worker_compute_time: float
    wall_clock_time: float
    throughput_iterations_per_sec: float
    summary: Dict[str, float]
    trajectories: Dict[str, List[float]]
    per_worker_contributions: Dict[str, int]
    completed_at: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class ResultAggregator:
    """
    Mengumpulkan dan menggabungkan hasil simulasi Monte Carlo SIR.
    Menerapkan validasi skema data, perlindungan idempotensi (anti-duplikasi),
    dan kalkulasi rata-rata tertimbang (weighted average) berdasarkan proporsi iterasi.
    """

    def __init__(self, expected_task_ids: List[str], total_expected_iterations: int):
        self._lock = threading.RLock()
        self.expected_task_ids: Set[str] = set(expected_task_ids)
        self.total_expected_iterations: int = total_expected_iterations
        
        # task_id -> TaskResult
        self._results: Dict[str, TaskResult] = {}
        
        # Timestamp awal komputasi untuk pengukuran wall-clock speedup
        self._start_time: float = time.time()

    def set_start_time(self, start_time: Optional[float] = None) -> None:
        """Menyetel waktu awal mulainya komputasi paralel/terdistribusi."""
        with self._lock:
            self._start_time = start_time or time.time()

    def validate_payload(self, payload: Dict[str, Any]) -> bool:
        """Memvalidasi struktur skema data hasil yang dikirim Worker."""
        required_keys = ["task_id", "worker_id", "iterations_completed", "simulation_summary", "trajectories"]
        for key in required_keys:
            if key not in payload:
                logger.error(f"Validasi gagal: Field '{key}' tidak ditemukan dalam payload result.")
                return False

        trajectories = payload.get("trajectories", {})
        if not all(k in trajectories for k in ["mean_s", "mean_i", "mean_r"]):
            logger.error("Validasi gagal: Trajectories wajib memuat 'mean_s', 'mean_i', dan 'mean_r'.")
            return False

        if payload.get("iterations_completed", 0) <= 0:
            logger.error("Validasi gagal: iterations_completed harus bernilai > 0.")
            return False

        return True

    def add_result(self, task_id: str, result_payload: Dict[str, Any]) -> bool:
        """
        Menambahkan hasil tugas dari Worker (Thread-Safe).
        
        Aturan Integritas:
        1. task_id harus terdaftar dalam expected_task_ids.
        2. Format payload harus memenuhi kontrak data simulasi.
        3. Idempotensi: Jika task_id sudah pernah diterima, tolak untuk mencegah perhitungan ganda.
        """
        with self._lock:
            # 1. Validasi keberadaan task_id
            if task_id not in self.expected_task_ids:
                logger.warning(f"Menolak hasil dari task tidak dikenal: {task_id}")
                return False

            # 2. Perlindungan anti-duplikasi (Idempotent)
            if task_id in self._results:
                logger.warning(f"Hasil duplikat untuk {task_id} diabaikan (sudah diagregasi sebelumnya).")
                return False

            # 3. Validasi skema
            if not self.validate_payload(result_payload):
                logger.error(f"Skema payload tidak valid untuk task {task_id}.")
                return False

            task_res = TaskResult(
                task_id=task_id,
                worker_id=result_payload["worker_id"],
                execution_time_seconds=float(result_payload.get("execution_time_seconds", 0.0)),
                iterations_completed=int(result_payload["iterations_completed"]),
                simulation_summary=result_payload["simulation_summary"],
                trajectories=result_payload["trajectories"]
            )

            self._results[task_id] = task_res
            logger.info(f"Hasil {task_id} berhasil dicatat dari {task_res.worker_id} ({task_res.iterations_completed} iterasi).")
            return True

    def is_complete(self) -> bool:
        """Memeriksa apakah seluruh expected task sudah berhasil dikumpulkan."""
        with self._lock:
            return len(self._results) == len(self.expected_task_ids)

    def get_missing_task_ids(self) -> List[str]:
        """Mengidentifikasi task mana saja yang belum selesai."""
        with self._lock:
            return list(self.expected_task_ids - set(self._results.keys()))

    def get_received_count(self) -> int:
        """Mendapatkan jumlah task yang sudah terkumpul."""
        with self._lock:
            return len(self._results)

    def aggregate(self) -> Optional[AggregatedSimulationResult]:
        """
        Menggabungkan seluruh hasil individual menjadi satu kesatuan statistik global.
        
        Kalkulasi Matematis:
        Setiap chunk k berkontribusi sebesar w_k = iterations_k / total_iterations.
        Kurva S(t), I(t), R(t) global dihitung sebagai rata-rata tertimbang:
        Curve_global(t) = sum(w_k * Curve_k(t))
        """
        with self._lock:
            if not self._results:
                logger.warning("Tidak ada data hasil untuk diagregasi.")
                return None

            total_iterations_collected = sum(r.iterations_completed for r in self._results.values())
            if total_iterations_collected == 0:
                return None

            total_worker_compute_time = sum(r.execution_time_seconds for r in self._results.values())
            wall_clock_time = max(0.0001, time.time() - self._start_time)
            throughput = total_iterations_collected / wall_clock_time

            # Hitung kontribusi per worker
            per_worker_contributions: Dict[str, int] = {}
            for r in self._results.values():
                per_worker_contributions[r.worker_id] = (
                    per_worker_contributions.get(r.worker_id, 0) + r.iterations_completed
                )

            # Inisialisasi struktur gabungan trajectories
            first_result = next(iter(self._results.values()))
            num_time_steps = len(first_result.trajectories["mean_s"])

            aggregated_s = [0.0] * num_time_steps
            aggregated_i = [0.0] * num_time_steps
            aggregated_r = [0.0] * num_time_steps

            # Inisialisasi ringkasan statistik
            aggregated_summary: Dict[str, float] = {}
            for key in first_result.simulation_summary.keys():
                aggregated_summary[key] = 0.0

            # Akumulasi rata-rata tertimbang (Weighted Average)
            for r in self._results.values():
                weight = r.iterations_completed / total_iterations_collected
                
                # Trajectories
                for t in range(num_time_steps):
                    aggregated_s[t] += weight * r.trajectories["mean_s"][t]
                    aggregated_i[t] += weight * r.trajectories["mean_i"][t]
                    aggregated_r[t] += weight * r.trajectories["mean_r"][t]

                # Summary metrics
                for key, val in r.simulation_summary.items():
                    if key in aggregated_summary:
                        aggregated_summary[key] += weight * float(val)

            # Bulatkan hasil ringkasan
            for key in aggregated_summary:
                aggregated_summary[key] = round(aggregated_summary[key], 4)

            aggregated_s = [round(x, 2) for x in aggregated_s]
            aggregated_i = [round(x, 2) for x in aggregated_i]
            aggregated_r = [round(x, 2) for x in aggregated_r]

            return AggregatedSimulationResult(
                total_tasks_completed=len(self._results),
                total_iterations=total_iterations_collected,
                total_worker_compute_time=round(total_worker_compute_time, 4),
                wall_clock_time=round(wall_clock_time, 4),
                throughput_iterations_per_sec=round(throughput, 2),
                summary=aggregated_summary,
                trajectories={
                    "mean_s": aggregated_s,
                    "mean_i": aggregated_i,
                    "mean_r": aggregated_r
                },
                per_worker_contributions=per_worker_contributions
            )

    def export_to_json(self, filepath: str) -> None:
        """Mengekspor hasil agregasi akhir ke file JSON."""
        with self._lock:
            aggregated = self.aggregate()
            if not aggregated:
                raise ValueError("Gagal melakukan ekspor: Belum ada data hasil yang terkumpul.")

            os.makedirs(os.path.dirname(os.path.abspath(filepath)), exist_ok=True)
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(aggregated.to_dict(), f, indent=2)
            logger.info(f"Hasil agregasi simulasi berhasil disimpan ke {filepath}")
