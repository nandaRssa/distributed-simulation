"""
Module: performance_monitor.py
Fokus: Perekaman performa sistem komputasi terdistribusi secara real-time.
       Mengukur Execution Time, CPU Utilization, Memory Utilization,
       Throughput (iterasi/detik), Speedup, Efisiensi, dan Recovery Time.
Bagian dari: System Optimization & Performance Evaluation (Anggota 3)
"""

import time
import logging
import threading
from typing import Dict, Any, List, Optional
import json

try:
    import psutil
    _HAS_PSUTIL = True
except ImportError:
    _HAS_PSUTIL = False

logger = logging.getLogger("PerformanceMonitor")


class SystemResourceSample:
    """Sampel titik waktu penggunaan sumber daya komputasi."""

    def __init__(self, timestamp: float, cpu_percent: float, memory_mb: float, memory_percent: float):
        self.timestamp = timestamp
        self.cpu_percent = cpu_percent
        self.memory_mb = memory_mb
        self.memory_percent = memory_percent

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": round(self.timestamp, 3),
            "cpu_percent": round(self.cpu_percent, 2),
            "memory_mb": round(self.memory_mb, 2),
            "memory_percent": round(self.memory_percent, 2)
        }


class PerformanceMonitor:
    """
    Monitor Pengukuran Kinerja untuk Proyek UAS Komputasi Paralel & Terdistribusi.
    Mengukur:
    1. Wall-Clock Execution Time (detik)
    2. CPU Utilization (Rata-rata & Puncak)
    3. Memory Utilization (Rata-rata & Puncak dalam MB dan %)
    4. Throughput = Total Iterasi / Wall-Clock Time
    5. Speedup S = T_sequential / T_parallel
    6. Efficiency E = S / P
    7. Recovery Time saat terjadi kegagalan worker
    """

    def __init__(self, sample_interval_seconds: float = 0.5):
        self.sample_interval = sample_interval_seconds
        self.samples: List[SystemResourceSample] = []
        self._is_monitoring = False
        self._monitor_thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()

        # Metrik Waktu
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.recovery_events: List[Dict[str, Any]] = []

    def start(self) -> None:
        """Memulai sesi pencatatan performa dan background resource sampling."""
        with self._lock:
            if self._is_monitoring:
                return
            self.samples.clear()
            self.recovery_events.clear()
            self.start_time = time.time()
            self.end_time = None
            self._is_monitoring = True

            if _HAS_PSUTIL:
                self._monitor_thread = threading.Thread(
                    target=self._sampling_loop,
                    name="PerformanceMonitor-Sampler",
                    daemon=True
                )
                self._monitor_thread.start()
                logger.info("[PERF MONITOR] Perekaman sumber daya dimulai.")
            else:
                logger.warning("[PERF MONITOR] psutil tidak ditemukan. Monitoring sumber daya dinonaktifkan.")

    def stop(self) -> None:
        """Menghentikan sesi pencatatan performa."""
        with self._lock:
            if not self._is_monitoring:
                return
            self.end_time = time.time()
            self._is_monitoring = False
            if self._monitor_thread and self._monitor_thread.is_alive():
                self._monitor_thread.join(timeout=1.0)
            self._monitor_thread = None
            logger.info(f"[PERF MONITOR] Perekaman selesai. Durasi: {self.get_wall_clock_time():.3f}s")

    def _sampling_loop(self) -> None:
        """Loop berkala untuk mencatat sampel CPU & RAM."""
        process = psutil.Process()
        # Initial call to prime psutil
        try:
            psutil.cpu_percent(interval=None)
        except Exception:
            pass

        while self._is_monitoring:
            try:
                cpu_sys = psutil.cpu_percent(interval=None)
                mem = psutil.virtual_memory()
                sample = SystemResourceSample(
                    timestamp=time.time(),
                    cpu_percent=cpu_sys,
                    memory_mb=mem.used / (1024 * 1024),
                    memory_percent=mem.percent
                )
                with self._lock:
                    self.samples.append(sample)
            except Exception as e:
                logger.debug(f"Gagal mengambil sampel resource: {e}")
            time.sleep(self.sample_interval)

    def record_recovery_event(self, task_id: str, failed_worker_id: str, duration_seconds: float) -> None:
        """Mencatat durasi pemulihan (Recovery Time) saat terjadi insiden kegagalan worker."""
        with self._lock:
            self.recovery_events.append({
                "task_id": task_id,
                "failed_worker_id": failed_worker_id,
                "recovery_time_seconds": round(duration_seconds, 4),
                "timestamp": time.time()
            })
            logger.info(
                f"[PERF MONITOR] Recovery Event dicatat: Task {task_id} "
                f"(Recovery Time: {duration_seconds:.3f}s)"
            )

    def get_wall_clock_time(self) -> float:
        """Mengembalikan total wall-clock time dalam detik."""
        with self._lock:
            if self.start_time is None:
                return 0.0
            end = self.end_time if self.end_time is not None else time.time()
            return max(0.0, end - self.start_time)

    def get_resource_summary(self) -> Dict[str, Any]:
        """Mengembalikan ringkasan statistik utilisasi CPU dan Memori."""
        with self._lock:
            if not self.samples:
                return {
                    "avg_cpu_percent": 0.0,
                    "peak_cpu_percent": 0.0,
                    "avg_memory_mb": 0.0,
                    "peak_memory_mb": 0.0,
                    "total_samples": 0
                }

            cpus = [s.cpu_percent for s in self.samples]
            mems = [s.memory_mb for s in self.samples]

            return {
                "avg_cpu_percent": round(sum(cpus) / len(cpus), 2),
                "peak_cpu_percent": round(max(cpus), 2),
                "avg_memory_mb": round(sum(mems) / len(mems), 2),
                "peak_memory_mb": round(max(mems), 2),
                "total_samples": len(self.samples)
            }

    def compute_evaluation_metrics(
        self,
        total_iterations: int,
        sequential_baseline_time: Optional[float] = None,
        num_processing_units: int = 1
    ) -> Dict[str, Any]:
        """
        Menghitung seluruh metrik standar UAS:
        Execution Time, Throughput, Speedup, Efficiency, Recovery Time, dan Resource Usage.
        
        Args:
            total_iterations: Total iterasi simulasi yang diselesaikan.
            sequential_baseline_time: Waktu eksekusi baseline serial (T_seq) dalam detik.
            num_processing_units: Jumlah unit pemrosesan (P) konsisten (misal: jumlah worker node).
        """
        wall_clock = self.get_wall_clock_time()
        throughput = (total_iterations / wall_clock) if wall_clock > 0 else 0.0
        resource_stats = self.get_resource_summary()

        speedup: Optional[float] = None
        efficiency: Optional[float] = None

        if sequential_baseline_time and sequential_baseline_time > 0 and wall_clock > 0:
            speedup = round(sequential_baseline_time / wall_clock, 3)
            p = max(1, num_processing_units)
            efficiency = round(speedup / p, 4)

        avg_recovery_time = 0.0
        if self.recovery_events:
            total_rec = sum(e["recovery_time_seconds"] for e in self.recovery_events)
            avg_recovery_time = round(total_rec / len(self.recovery_events), 4)

        return {
            "total_iterations": total_iterations,
            "wall_clock_time_seconds": round(wall_clock, 4),
            "throughput_iterations_per_sec": round(throughput, 2),
            "num_processing_units_P": num_processing_units,
            "sequential_baseline_time_seconds": round(sequential_baseline_time, 4) if sequential_baseline_time else None,
            "speedup": speedup,
            "efficiency": efficiency,
            "cpu_utilization": {
                "avg_percent": resource_stats["avg_cpu_percent"],
                "peak_percent": resource_stats["peak_cpu_percent"]
            },
            "memory_utilization": {
                "avg_mb": resource_stats["avg_memory_mb"],
                "peak_mb": resource_stats["peak_memory_mb"]
            },
            "fault_recovery": {
                "total_recovery_events": len(self.recovery_events),
                "avg_recovery_time_seconds": avg_recovery_time,
                "events": self.recovery_events
            }
        }

    def export_report_to_json(self, output_filepath: str, metrics: Dict[str, Any]) -> None:
        """Menyimpan laporan evaluasi performa ke file JSON untuk keperluan BAB IV."""
        with open(output_filepath, "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2)
        logger.info(f"[PERF MONITOR] Laporan performa diekspor ke {output_filepath}")

