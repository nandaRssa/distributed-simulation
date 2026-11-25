"""
Module: optimization_hooks.py
Fokus: Jembatan kompatibilitas antarmuka untuk Dynamic Load Balancing
       dan Fault Tolerance (Menggunakan modul implementasi Anggota 3).
Bagian dari: Master Controller (Anggota 1 & Anggota 3 Interface)
"""

from master.load_balancer import DynamicLoadBalancer
from master.fault_tolerance import FaultToleranceManager

# Alias kompatibilitas untuk antarmuka strategi lama
DynamicLoadBalancingStrategy = DynamicLoadBalancer

__all__ = [
    "DynamicLoadBalancer",
    "DynamicLoadBalancingStrategy",
    "FaultToleranceManager",
]
