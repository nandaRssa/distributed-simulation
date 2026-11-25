"""Utilitas konfigurasi simulasi SIR tanpa duplikasi definisi dataclass."""

from dataclasses import asdict
from typing import Any, Dict

from master.task_decomposer import SimulationConfig

# Profil baseline berat untuk demonstrasi manfaat paralel.
# Nilai ini tidak memaksa konfigurasi master, hanya panduan default benchmark.
HEAVY_BASELINE_DEFAULTS: Dict[str, Any] = {
    "iterations": 120000,
    "population": 50000,
    "time_steps": 240,
    "initial_infected": 25,
    "initial_recovered": 0,
    "beta": 0.28,
    "gamma": 0.09,
}


def normalize_config(config: Any) -> SimulationConfig:
    """Normalisasi input konfigurasi ke objek SimulationConfig yang tervalidasi."""
    if isinstance(config, SimulationConfig):
        cfg = config
    elif isinstance(config, dict):
        cfg = SimulationConfig(**config)
    else:
        raise TypeError("config harus berupa dict atau SimulationConfig.")

    cfg.validate()
    return cfg


def config_to_dict(config: Any) -> Dict[str, Any]:
    """Konversi konfigurasi tervalidasi ke dictionary serializable."""
    return asdict(normalize_config(config))


def get_heavy_baseline_defaults() -> Dict[str, Any]:
    """Mengembalikan preset parameter baseline berat untuk uji performa worker."""
    return dict(HEAVY_BASELINE_DEFAULTS)


__all__ = [
    "SimulationConfig",
    "normalize_config",
    "config_to_dict",
    "HEAVY_BASELINE_DEFAULTS",
    "get_heavy_baseline_defaults",
]
