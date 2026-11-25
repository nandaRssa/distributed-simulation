"""
Module: master.py
Fokus: Program utama (Master Controller) yang mengorkestrasikan seluruh modul:
       TCP Server, Worker Manager, Task Decomposer, Task Queue, Task Scheduler,
       dan Result Aggregator dalam satu siklus hidup terintegrasi.
Bagian dari: Master Controller (Anggota 1)
"""

import sys
import os
import time
import socket
import logging
import argparse
import threading
from typing import Optional, Dict, Any, List

# Menambahkan root folder ke sys.path jika dijalankan langsung
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from shared.protocol import MessageType, ProtocolMessage
from master.task_decomposer import TaskDecomposer, Task, SimulationConfig
from master.task_queue import TaskQueue, TaskStatus
from master.task_scheduler import TaskScheduler, BaseSchedulingStrategy
from master.worker_manager import WorkerManager, WorkerInfo
from master.communication import MasterServer
from master.result_aggregator import ResultAggregator, AggregatedSimulationResult
from master.load_balancer import DynamicLoadBalancer
from master.fault_tolerance import FaultToleranceManager
from master.performance_monitor import PerformanceMonitor

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("MasterController")


class MasterController:
    """
    Orkestrator Utama Node Master.
    Mengatur proses inisialisasi jaringan, dekomposisi beban kerja, sinkronisasi
    multi-worker, alokasi task, pengumpulan hasil, dan shutdown teratur.
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 5000,
        total_iterations: int = 10000,
        chunk_size: int = 1000,
        min_workers: int = 9,
        simulation_config: Optional[SimulationConfig] = None,
        output_file: str = "results/simulation_result.json",
        scheduler_strategy: Optional[BaseSchedulingStrategy] = None,
        enable_fault_tolerance: bool = True,
        worker_timeout_seconds: float = 10.0
    ):
        self.host = host
        self.port = port
        self.total_iterations = total_iterations
        self.chunk_size = chunk_size
        self.min_workers = min_workers
        self.sim_config = simulation_config or SimulationConfig()
        self.output_file = output_file
        self.enable_fault_tolerance = enable_fault_tolerance

        # Komponen internal Master
        self.decomposer = TaskDecomposer(base_seed=42)
        self.task_queue = TaskQueue()
        self.worker_manager = WorkerManager()
        self.load_balancer = DynamicLoadBalancer(worker_manager=self.worker_manager)
        active_strategy = scheduler_strategy if scheduler_strategy is not None else self.load_balancer
        self.scheduler = TaskScheduler(task_queue=self.task_queue, strategy=active_strategy)
        self.server = MasterServer(host=self.host, port=self.port)
        self.aggregator: Optional[ResultAggregator] = None

        # Modul Optimasi & Monitoring (Anggota 3)
        self.performance_monitor = PerformanceMonitor(sample_interval_seconds=0.5)
        self.fault_tolerance_manager = FaultToleranceManager(
            task_queue=self.task_queue,
            worker_manager=self.worker_manager,
            timeout_threshold_seconds=worker_timeout_seconds,
            check_interval_seconds=1.0,
            on_task_recovered_callback=self._on_task_recovered
        )

        # State & Synchronization
        self._all_tasks: List[Task] = []
        self._simulation_completed_event = threading.Event()
        self._is_distributing = False
        self._lock = threading.RLock()

    def _on_task_recovered(self, task_id: str, failed_worker_id: str) -> None:
        """Callback saat task berhasil dipulihkan akibat worker crash / timeout."""
        logger.info(f"[RECOVERY] Task {task_id} siap ditugaskan kembali dari worker {failed_worker_id}")
        self.performance_monitor.record_recovery_event(task_id, failed_worker_id, 0.0)
        with self._lock:
            # Cari worker yang sedang READY untuk langsung mengerjakan task hasil recovery
            for worker in self.worker_manager.get_ready_workers():
                if self._dispatch_task_to_worker(worker):
                    break

    def initialize_simulation(self) -> None:
        """Melakukan dekomposisi task dan menyiapkan antrean serta agregator."""
        logger.info(f"Menginisialisasi simulasi: {self.total_iterations} iterasi (Chunk: {self.chunk_size})")
        logger.info(
            "Konfigurasi simulasi aktif: "
            f"population={self.sim_config.population}, "
            f"initial_infected={self.sim_config.initial_infected}, "
            f"initial_recovered={self.sim_config.initial_recovered}, "
            f"beta={self.sim_config.beta}, "
            f"gamma={self.sim_config.gamma}, "
            f"time_steps={self.sim_config.time_steps}"
        )
        self._all_tasks = self.decomposer.decompose(
            total_iterations=self.total_iterations,
            chunk_size=self.chunk_size,
            config=self.sim_config
        )
        
        self.task_queue.add_tasks(self._all_tasks)
        task_ids = [t.task_id for t in self._all_tasks]
        self.aggregator = ResultAggregator(
            expected_task_ids=task_ids,
            total_expected_iterations=self.total_iterations
        )
        logger.info(f"Berhasil membuat {len(self._all_tasks)} paket task independen.")

    def _setup_handlers(self) -> None:
        """Mendaftarkan handler event protokol komunikasi MasterServer."""
        self.server.register_handler(MessageType.REGISTER, self._handle_register)
        self.server.register_handler(MessageType.STATUS, self._handle_status)
        self.server.register_handler(MessageType.RESULT, self._handle_result)
        self.server.register_handler(MessageType.HEARTBEAT, self._handle_heartbeat)

        self.server.set_connection_callbacks(
            on_connect=self._on_client_connect,
            on_disconnect=self._on_client_disconnect
        )

    def _on_client_connect(self, sock: socket.socket, addr: tuple) -> None:
        logger.info(f"Worker fisik terhubung dari {addr[0]}:{addr[1]}")

    def _on_client_disconnect(self, sock: socket.socket, addr: tuple) -> None:
        with self._lock:
            disconnected_worker = self.worker_manager.mark_disconnected(addr)
            if disconnected_worker:
                logger.warning(
                    f"Worker {disconnected_worker.worker_id} ({addr[0]}:{addr[1]}) terputus."
                )
                if self.enable_fault_tolerance and self._is_distributing:
                    self.fault_tolerance_manager.check_and_recover()

    def _handle_register(self, sock: socket.socket, addr: tuple, msg: Dict[str, Any]) -> None:
        """Menangani pendaftaran Worker baru."""
        payload = msg.get("payload", {})
        worker_id = payload.get("worker_id", f"WORKER-{addr[0]}:{addr[1]}")
        cpu_cores = payload.get("cores", 1)

        with self._lock:
            worker = self.worker_manager.register_worker(
                worker_id=worker_id,
                ip=addr[0],
                port=addr[1],
                client_sock=sock,
                cpu_cores=cpu_cores,
                metadata=payload.get("metadata", {})
            )
            logger.info(f"Worker terdaftar: {worker_id} ({cpu_cores} Cores) dari {addr[0]}:{addr[1]}")

            # Balas REGISTER_ACK
            ack = ProtocolMessage.create(
                MessageType.REGISTER_ACK,
                {
                    "status": "ACCEPTED",
                    "assigned_id": worker_id,
                    "master_time": time.time()
                }
            )
            self.server.send_message(sock, ack)

            # Jika simulasi sudah aktif berjalan dan ada task pending, alokasikan langsung
            if self._is_distributing and self.task_queue.has_pending_tasks():
                self._dispatch_task_to_worker(worker)

    def _handle_status(self, sock: socket.socket, addr: tuple, msg: Dict[str, Any]) -> None:
        """Menangani pembaruan status dari Worker (READY / BUSY)."""
        payload = msg.get("payload", {})
        status = payload.get("status", "READY")
        worker = self.worker_manager.get_worker_by_address(addr)

        if not worker:
            logger.warning(f"Status diterima dari worker tidak terdaftar: {addr}")
            return

        with self._lock:
            self.worker_manager.update_work_status(worker.worker_id, status)
            if status == "READY" and self._is_distributing and self.task_queue.has_pending_tasks():
                self._dispatch_task_to_worker(worker)

    def _handle_result(self, sock: socket.socket, addr: tuple, msg: Dict[str, Any]) -> None:
        """Menangani penerimaan hasil komputasi task dari Worker."""
        payload = msg.get("payload", {})
        task_id = payload.get("task_id")
        worker_id = payload.get("worker_id")
        exec_time = payload.get("execution_time_seconds", 0.0)

        with self._lock:
            # 1. Catat ke Result Aggregator
            added = self.aggregator.add_result(task_id, payload)
            
            # 2. Perbarui Scheduler & Worker Manager & Dynamic Load Balancer
            if added:
                self.scheduler.record_task_completion(task_id, worker_id, exec_time)
                self.worker_manager.complete_task(worker_id, task_id, exec_time)
                iters_completed = payload.get("iterations_completed", self.chunk_size)
                self.load_balancer.record_task_result(worker_id, iters_completed, exec_time)
            
            # 3. Kirim konfirmasi RESULT_ACK ke Worker
            ack = ProtocolMessage.create(
                MessageType.RESULT_ACK,
                {"task_id": task_id, "status": "RECEIVED" if added else "DUPLICATE"}
            )
            self.server.send_message(sock, ack)

            # 4. Tampilkan progres di console
            summary = self.task_queue.get_summary()
            logger.info(
                f"[PROGRES] Selesai: {summary['completed']}/{summary['total']} "
                f"({summary['progress_percent']}%) | Worker: {worker_id}"
            )

            # 5. Cek kelengkapan seluruh task
            if self.aggregator.is_complete():
                logger.info("Seluruh task simulasi telah berhasil diselesaikan!")
                self._simulation_completed_event.set()
                return

            # 6. Jika masih ada task pending, langsung beri tugas baru ke worker ini
            worker = self.worker_manager.get_worker(worker_id)
            if worker and self.task_queue.has_pending_tasks():
                self._dispatch_task_to_worker(worker)

    def _handle_heartbeat(self, sock: socket.socket, addr: tuple, msg: Dict[str, Any]) -> None:
        """Menangani detak jantung (heartbeat) dari worker."""
        worker = self.worker_manager.get_worker_by_address(addr)
        if worker:
            self.worker_manager.update_heartbeat(worker.worker_id)
            ack = ProtocolMessage.create(MessageType.HEARTBEAT_ACK, {"timestamp": time.time()})
            self.server.send_message(sock, ack)

    def _dispatch_task_to_worker(self, worker: WorkerInfo) -> bool:
        """Mengirimkan task berikutnya ke Worker tertentu."""
        if not worker.socket or worker.connection_status != "CONNECTED":
            return False

        task = self.scheduler.dispatch_next_task(
            worker_id=worker.worker_id,
            worker_info=worker.to_dict()
        )
        if not task:
            return False

        self.worker_manager.assign_task(worker.worker_id, task.task_id)

        task_msg = ProtocolMessage.create(
            MessageType.TASK,
            {
                "task_id": task.task_id,
                "iterations": task.iterations,
                "simulation_config": task.simulation_config,
                "random_seed": task.random_seed
            }
        )
        success = self.server.send_message(worker.socket, task_msg)
        if success:
            logger.info(f"Task {task.task_id} ({task.iterations} iterasi) dikirim ke {worker.worker_id}")
        else:
            logger.error(f"Gagal mengirim task {task.task_id} ke {worker.worker_id}")
            self.task_queue.requeue_task(task.task_id)
        return success

    def wait_for_workers(self, target_count: int, timeout_seconds: float = 60.0) -> bool:
        """Menunggu hingga jumlah minimal worker fisik terhubung."""
        logger.info(f"Menunggu {target_count} worker terhubung ke {self.host}:{self.port}...")
        start_wait = time.time()
        while time.time() - start_wait < timeout_seconds:
            ready_workers = len(self.worker_manager.get_connected_workers())
            if ready_workers >= target_count:
                logger.info(f"Target worker tercapai! ({ready_workers} worker terhubung)")
                return True
            time.sleep(0.5)

        current = len(self.worker_manager.get_connected_workers())
        logger.warning(f"Batas waktu tunggu habis. Terhubung: {current}/{target_count} worker.")
        return current >= target_count

    def start(self, wait_timeout: float = 300.0) -> Optional[AggregatedSimulationResult]:
        """
        Menjalankan siklus eksekusi simulasi terdistribusi secara penuh:
        1. Inisialisasi task.
        2. Jalankan Master TCP Server.
        3. Tunggu worker fisik terhubung.
        4. Mulai alokasi dan distribusi paralel.
        5. Tunggu seluruh hasil terkumpul.
        6. Agregasi kurva SIR & simpan ke file.
        7. Hentikan worker & matikan server.
        """
        self.initialize_simulation()
        self._setup_handlers()
        self.server.start()

        # Tunggu worker terhubung
        connected = self.wait_for_workers(self.min_workers, timeout_seconds=900.0)
        if not connected:
            logger.error(
                f"Jumlah worker kurang dari minimum ({self.min_workers}). Simulasi dibatalkan."
            )
            self.server.stop()
            return None

        # Catat waktu mulai komputasi aktif
        logger.info("Memulai distribusi task simulasi...")
        self._is_distributing = True
        self.aggregator.set_start_time()
        self.performance_monitor.start()

        if self.enable_fault_tolerance:
            self.fault_tolerance_manager.start()

        # Alokasikan task awal ke seluruh worker yang siap
        with self._lock:
            for worker in self.worker_manager.get_ready_workers():
                self._dispatch_task_to_worker(worker)

        # Tunggu sampai seluruh task selesai
        completed = self._simulation_completed_event.wait(timeout=wait_timeout)
        if not completed:
            logger.warning("Batas waktu simulasi (timeout) tercapai sebelum seluruh task selesai.")

        self.performance_monitor.stop()

        if self.enable_fault_tolerance:
            self.fault_tolerance_manager.stop()

        # Lakukan agregasi hasil
        aggregated = self.aggregator.aggregate()
        if aggregated:
            self.aggregator.export_to_json(self.output_file)
            perf_stats = self.performance_monitor.compute_evaluation_metrics(
                total_iterations=aggregated.total_iterations,
                num_processing_units=len(self.worker_manager.get_all_workers())
            )
            logger.info("=" * 60)
            logger.info("RINGKASAN HASIL SIMULASI MONTE CARLO TERDISTRIBUSI")
            logger.info("=" * 60)
            logger.info(f"Total Iterasi              : {aggregated.total_iterations}")
            logger.info(f"Total Task Selesai         : {aggregated.total_tasks_completed}")
            logger.info(f"Waktu Komputasi Worker (s) : {aggregated.total_worker_compute_time:.2f} s")
            logger.info(f"Wall-Clock Time (s)        : {aggregated.wall_clock_time:.2f} s")
            logger.info(f"Throughput                 : {aggregated.throughput_iterations_per_sec:.2f} iterasi/detik")
            logger.info(f"Rata-rata CPU Utilization  : {perf_stats['cpu_utilization']['avg_percent']:.1f}% (Puncak: {perf_stats['cpu_utilization']['peak_percent']:.1f}%)")
            logger.info(f"Rata-rata Memory (RAM)     : {perf_stats['memory_utilization']['avg_mb']:.1f} MB (Puncak: {perf_stats['memory_utilization']['peak_mb']:.1f} MB)")
            if perf_stats['fault_recovery']['total_recovery_events'] > 0:
                logger.info(f"Total Pemulihan Task       : {perf_stats['fault_recovery']['total_recovery_events']}")
            logger.info(f"Puncak Terinfeksi (Mean)   : {aggregated.summary.get('peak_infected_mean', 0.0):.2f}")
            logger.info(f"Hari Puncak (Mean)         : {aggregated.summary.get('peak_day_mean', 0.0):.2f}")
            logger.info(f"File Hasil Disimpan Pada   : {self.output_file}")
            logger.info("=" * 60)

        # Hentikan worker & server secara teratur
        self.stop()
        return aggregated

    def stop(self) -> None:
        """Menghentikan Master Controller dan seluruh worker terhubung."""
        with self._lock:
            self._is_distributing = False
            if self.enable_fault_tolerance:
                self.fault_tolerance_manager.stop()
            self.server.stop()
        logger.info("Master Controller berhasil dihentikan secara teratur.")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Master Controller - Distributed Monte Carlo Epidemic Simulation")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="IP address untuk listen socket (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=5000, help="Port komunikasi TCP (default: 5000)")
    parser.add_argument("--iterations", type=int, default=None, help="Total iterasi Monte Carlo (default: 10000, atau preset heavy jika dipilih)")
    parser.add_argument("--chunk-size", type=int, default=1000, help="Jumlah iterasi per paket task (default: 1000)")
    parser.add_argument("--min-workers", type=int, default=9, help="Jumlah minimal worker yang ditunggu sebelum mulai (default: 9)")
    parser.add_argument("--output", type=str, default="results/simulation_result.json", help="Path file output JSON")
    parser.add_argument("--preset", type=str, choices=["heavy"], default=None, help="Preset konfigurasi simulasi")
    parser.add_argument("--population", type=int, default=None, help="Total populasi model SIR")
    parser.add_argument("--time-steps", type=int, default=None, help="Jumlah langkah waktu simulasi")
    parser.add_argument("--initial-infected", type=int, default=None, help="Jumlah terinfeksi awal")
    parser.add_argument("--beta", type=float, default=None, help="Laju penularan")
    parser.add_argument("--gamma", type=float, default=None, help="Laju kesembuhan")
    return parser.parse_args(argv)


def build_simulation_config_from_args(args: argparse.Namespace) -> SimulationConfig:
    """Menyusun SimulationConfig dari default, preset, dan override eksplisit CLI."""
    defaults = SimulationConfig()
    base: Dict[str, Any] = {}

    if args.preset == "heavy":
        from simulation.simulation_config import get_heavy_baseline_defaults
        heavy = get_heavy_baseline_defaults()
        base = {
            "population": heavy["population"],
            "initial_infected": heavy["initial_infected"],
            "beta": heavy["beta"],
            "gamma": heavy["gamma"],
            "time_steps": heavy["time_steps"],
        }

    return SimulationConfig(
        population=args.population if args.population is not None else base.get("population", defaults.population),
        initial_infected=(
            args.initial_infected
            if args.initial_infected is not None
            else base.get("initial_infected", defaults.initial_infected)
        ),
        initial_recovered=defaults.initial_recovered,
        beta=args.beta if args.beta is not None else base.get("beta", defaults.beta),
        gamma=args.gamma if args.gamma is not None else base.get("gamma", defaults.gamma),
        time_steps=args.time_steps if args.time_steps is not None else base.get("time_steps", defaults.time_steps),
    )


def resolve_total_iterations(args: argparse.Namespace) -> int:
    """Menentukan total iterasi dari default, preset, dan override eksplisit CLI."""
    if args.iterations is not None:
        return args.iterations
    if args.preset == "heavy":
        from simulation.simulation_config import get_heavy_baseline_defaults
        return int(get_heavy_baseline_defaults()["iterations"])
    return 10000


if __name__ == "__main__":
    args = parse_args()
    sim_config = build_simulation_config_from_args(args)
    controller = MasterController(
        host=args.host,
        port=args.port,
        total_iterations=resolve_total_iterations(args),
        chunk_size=args.chunk_size,
        min_workers=args.min_workers,
        simulation_config=sim_config,
        output_file=args.output
    )
    try:
        controller.start()
    except KeyboardInterrupt:
        logger.info("Interupsi manual diterima (Ctrl+C). Menghentikan sistem...")
        controller.stop()
