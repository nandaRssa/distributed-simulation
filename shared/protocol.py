"""
Module: protocol.py
Fokus: Standarisasi format pesan dan tipe protokol komunikasi TCP Master-Worker berbasis JSON.
Bagian dari: Shared Module (Digunakan oleh Master dan Worker)
"""

from enum import Enum
from typing import Dict, Any, Optional


class MessageType(str, Enum):
    """Katalog tipe pesan yang disepakati dalam protokol Master-Worker."""
    REGISTER = "REGISTER"            # Worker -> Master: Registrasi awal identitas worker
    REGISTER_ACK = "REGISTER_ACK"    # Master -> Worker: Konfirmasi penerimaan registrasi
    STATUS = "STATUS"                # Worker -> Master: Pembaruan status (READY, BUSY, IDLE)
    TASK = "TASK"                    # Master -> Worker: Penugasan paket simulasi Monte Carlo
    RESULT = "RESULT"                # Worker -> Master: Pengiriman hasil komputasi task
    RESULT_ACK = "RESULT_ACK"        # Master -> Worker: Konfirmasi hasil telah diterima & diagregasi
    HEARTBEAT = "HEARTBEAT"          # Master <-> Worker: Probe deteksi koneksi (Anggota 3)
    HEARTBEAT_ACK = "HEARTBEAT_ACK"  # Master <-> Worker: Respon probe deteksi
    STOP = "STOP"                    # Master -> Worker: Instruksi penghentian worker secara teratur
    ERROR = "ERROR"                  # Dua arah: Notifikasi anomali protokol/eksekusi


class ProtocolMessage:
    """Helper untuk validasi dan standarisasi pembuatan payload pesan JSON."""

    @staticmethod
    def create(msg_type: MessageType, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Membuat struktur envelope pesan standar."""
        return {
            "type": msg_type.value if isinstance(msg_type, MessageType) else str(msg_type),
            "payload": payload or {}
        }

    @staticmethod
    def validate(message: Dict[str, Any]) -> bool:
        """Memeriksa apakah dictionary memiliki format envelope yang valid."""
        return isinstance(message, dict) and "type" in message and "payload" in message
