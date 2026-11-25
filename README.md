# Distributed Monte Carlo Epidemic Simulation Using Master–Worker Architecture

Implementasi sistem komputasi terdistribusi untuk simulasi penyebaran epidemi model **SIR (Susceptible, Infected, Recovered)** menggunakan **Monte Carlo Simulation** dan arsitektur **Master–Worker**.

Proyek ini dibangun untuk memenuhi kriteria UAS Komputasi Paralel dan Sistem Terdistribusi (Topik 19 - Distributed Simulation) dengan ketentuan **10 Komputer Fisik (1 Master Node + 9 Worker Nodes)**.

---

## 1. Arsitektur Komponen & Pembagian Jobdesk

Struktur folder proyek sesuai dengan dokumen spesifikasi UAS:

```
Distributed_Simulation/
│
├── master/                         # Tanggung Jawab Anggota 1 & Anggota 3
│   ├── master.py                   # Main Controller & CLI (Anggota 1)
│   ├── communication.py            # TCP Server, Thread-per-Client, Message Framing (Anggota 1)
│   ├── task_decomposer.py          # Partisi Iterasi Presisi & Random Seed Unik (Anggota 1)
│   ├── task_queue.py               # Thread-Safe Task Queue (PENDING, RUNNING, COMPLETED) (Anggota 1)
│   ├── task_scheduler.py           # Scheduler Orkestrator (Strategy Pattern) (Anggota 1)
│   ├── worker_manager.py           # Registry Node Worker Fisik & Tracking Status (Anggota 1)
│   ├── result_aggregator.py        # Validasi, Idempotensi, & Agregasi Weighted Average (Anggota 1)
│   ├── load_balancer.py            # Dynamic Load Balancing adaptif throughput & core (Anggota 3)
│   ├── fault_tolerance.py          # Deteksi kegagalan node, timeout, retry & blacklist (Anggota 3)
│   └── performance_monitor.py      # Real-time resource sampling CPU, RAM, Speedup, Efisiensi (Anggota 3)
│
├── shared/                         # Modul Bersama Master & Worker
│   ├── protocol.py                 # Katalog Tipe Pesan & Protocol Envelope
│   └── message_framing.py          # 4-Byte Length-Prefixed Binary Framing
│
├── simulation/                     # Tanggung Jawab Anggota 2 (Engine Simulasi)
│   ├── sir_model.py                # Model SIR chain-binomial stokastik berbasis NumPy
│   ├── monte_carlo.py              # Runner chunk Monte Carlo
│   └── simulation_config.py        # Dataclass konfigurasi SIR + preset baseline berat
│
├── worker/                         # Tanggung Jawab Anggota 2
│   ├── worker.py                   # Client Worker Node + background heartbeat
│   ├── multiprocessing_engine.py   # Multi-core sub-chunk parallelism via multiprocessing.Pool
│   └── simulation_engine.py        # Orkestrator eksekusi task & formatting RESULT
│
├── benchmark/                      # Tanggung Jawab Anggota 3 (Suite Pengujian UAS)
│   ├── sequential.py               # Baseline single-process 1 core (T_seq)
│   ├── local_parallel.py           # Multiprocessing 1 komputer fisik (1, 2, 4, 8 core)
│   ├── distributed.py              # Benchmark simulasi terdistribusi Master-Worker
│   └── scalability_test.py         # Pengujian skalabilitas 1, 2, 4, 6, dan 9 Worker (JSON & CSV)
│
├── visualization/                  # Tanggung Jawab Anggota 3 (Visualisasi Laporan BAB IV)
│   ├── epidemic_chart.py           # Grafik kurva epidemiologi S(t), I(t), R(t) (300 DPI)
│   └── performance_chart.py        # Grafik Speedup, Efisiensi, Throughput, & Execution Time
│
├── tests/                          # Rangkaian Pengujian Lengkap (Unit & Integration Tests)
│   ├── test_task_decomposer.py
│   ├── test_task_queue.py
│   ├── test_task_scheduler.py
│   ├── test_communication.py
│   ├── test_worker_manager.py
│   ├── test_result_aggregator.py
│   ├── test_end_to_end_integration.py
│   ├── test_optimization_integration.py
│   ├── test_scalability_multi_workers.py
│   ├── test_sir_model.py
│   ├── test_monte_carlo.py
│   ├── test_multiprocessing_engine.py
│   └── test_anggotatiga_components.py
│
├── results/                        # Output Laporan, Data CSV, dan Gambar Grafik
├── requirements.txt
└── README.md
```

---

## 2. Panduan Menjalankan pada 10 Komputer Fisik (LAN)

Pastikan seluruh 10 komputer fisik terhubung pada satu jaringan switch LAN atau Wi-Fi yang sama (misal subnet `192.168.1.0/24`).

### Langkah 1: Jalankan Master pada Komputer Fisik 1
Cari tahu IP Address komputer Master (misal: `192.168.1.10` via `ipconfig` di Windows).
Buka terminal dan jalankan Master:
```powershell
python Distributed_Simulation/master/master.py --host 0.0.0.0 --port 5000 --iterations 100000 --chunk-size 5000 --min-workers 9
```
*Keterangan argumen:*
- `--host 0.0.0.0`: Mendengarkan seluruh interface jaringan fisik.
- `--port 5000`: Port komunikasi TCP.
- `--iterations 100000`: Total iterasi Monte Carlo yang akan disimulasikan.
- `--chunk-size 5000`: Ukuran iterasi per task (menghasilkan 20 task independen).
- `--min-workers 9`: Master akan menunggu sampai 9 Worker fisik terhubung sebelum mendistribusikan komputasi.

### Langkah 2: Jalankan 9 Worker pada Komputer Fisik 2 s.d. 10
Pada masing-masing komputer Worker, jalankan:
```powershell
# Komputer Fisik 2:
python Distributed_Simulation/worker/worker.py --host 192.168.1.10 --port 5000 --id WORKER-PHYSICAL-01

# Komputer Fisik 3:
python Distributed_Simulation/worker/worker.py --host 192.168.1.10 --port 5000 --id WORKER-PHYSICAL-02

# ... Lakukan hal serupa hingga Komputer Fisik 10 (WORKER-PHYSICAL-09)
```

Untuk mengatur jumlah proses lokal per worker (default: seluruh CPU logical):
```powershell
python Distributed_Simulation/worker/worker.py --host 192.168.1.10 --port 5000 --id WORKER-PHYSICAL-01 --processes 8
```

### Langkah 3: Pengamatan & Keluaran
1. Master akan mendeteksi seluruh koneksi masuk dan menampilkan progres komputasi real-time:
   `[PROGRES] Selesai: X/20 (Y%) | Worker: WORKER-PHYSICAL-XX`
2. Setelah seluruh task selesai, Master secara otomatis menggabungkan kurva $S(t)$, $I(t)$, $R(t)$ menggunakan metode **Weighted Average**.
3. Hasil akhir disimpan pada `results/simulation_result.json`.
4. Seluruh worker menerima sinyal `STOP` dan berhenti secara teratur (*graceful shutdown*).

---

## 3. Menjalankan Suite Benchmark & Evaluasi Skalabilitas

Untuk mengambil data pengujian performa yang diperlukan pada **BAB IV Laporan**:

### A. Benchmark Skalabilitas Otomatis (1, 2, 4, 6, 9 Worker)
```powershell
python Distributed_Simulation/benchmark/scalability_test.py --iterations 18000 --chunk-size 1000
```
*Keluaran:*
- File `results/scalability_results.json`: Laporan lengkap metrik performa.
- File `results/scalability_results.csv`: Tabel angka Speedup, Efisiensi, Throughput, CPU/RAM untuk laporan.

### B. Benchmark Sequential (Baseline 1 Core)
```powershell
python Distributed_Simulation/benchmark/sequential.py --iterations 10000
```

### C. Benchmark Local Parallel (Multiprocessing 1 Komputer)
```powershell
python Distributed_Simulation/benchmark/local_parallel.py --iterations 10000 --processes 1 2 4 8
```

---

## 4. Visualisasi Grafik untuk Laporan BAB IV

Setelah pengujian dijalankan, generate gambar grafik beresolusi tinggi (300 DPI):

```powershell
# 1. Grafik Kurva Epidemiologi S(t), I(t), R(t)
python Distributed_Simulation/visualization/epidemic_chart.py --input results/simulation_result.json --output results/epidemic_curve.png

# 2. Grafik Speedup, Efisiensi, Throughput, dan Execution Time
python Distributed_Simulation/visualization/performance_chart.py --input results/scalability_results.json
```

---

## 5. Menjalankan Rangkaian Pengujian (Automated Tests)
Untuk menjalankan seluruh unit dan integration test:
```powershell
python -m pytest Distributed_Simulation/tests
```
