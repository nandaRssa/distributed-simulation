"""
Module: message_framing.py
Fokus: Mekanisme Message Framing biner (4-Byte Length-Prefixed) untuk mengatasi
       masalah TCP Stream Fragmentation (paket terpotong atau menyatu).
Bagian dari: Shared Module (Digunakan oleh Master dan Worker)
"""

import json
import struct
import socket
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger("MessageFraming")

# 4 Byte unsigned int dalam big-endian network byte order (maksimal 4 GB payload)
HEADER_FORMAT = "!I"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)


class FramingError(Exception):
    """Exception khusus kegagalan framing atau protokol TCP."""
    pass


def send_framed_message(sock: socket.socket, message: Dict[str, Any]) -> None:
    """
    Mengemas pesan JSON ke dalam format Length-Prefixed Framing dan mengirimkannya
    seluruhnya melalui socket TCP.
    
    Format:
    [4 Byte Header: Panjang Payload (N)] [N Byte Payload: JSON UTF-8]
    """
    try:
        json_bytes = json.dumps(message, ensure_ascii=False).encode("utf-8")
        payload_length = len(json_bytes)
        
        # Buat 4 byte header
        header = struct.pack(HEADER_FORMAT, payload_length)
        
        # Kirim header + payload secara utuh menggunakan sendall
        sock.sendall(header + json_bytes)
    except (socket.error, BrokenPipeError, ConnectionResetError) as e:
        logger.error(f"Gagal mengirim pesan melalui socket: {e}")
        raise


def recv_exact(sock: socket.socket, num_bytes: int) -> Optional[bytes]:
    """
    Membaca socket secara persis sebanyak `num_bytes` byte.
    Menghindari asumsi satu kali recv() selalu menerima seluruh data.
    
    Mengembalikan None jika socket ditutup secara bersih oleh remote peer (EOF).
    """
    buffer = bytearray()
    while len(buffer) < num_bytes:
        bytes_to_read = num_bytes - len(buffer)
        try:
            chunk = sock.recv(bytes_to_read)
        except socket.timeout:
            raise
        except (socket.error, ConnectionResetError) as e:
            logger.warning(f"Koneksi terputus saat membaca socket: {e}")
            raise

        if not chunk:
            # Remote peer menutup koneksi (EOF)
            if len(buffer) == 0:
                return None
            raise FramingError(
                f"Koneksi tertutup sebelum pesan lengkap diterima. "
                f"Diharapkan {num_bytes} byte, baru diterima {len(buffer)} byte."
            )
        buffer.extend(chunk)

    return bytes(buffer)


def recv_framed_message(sock: socket.socket) -> Optional[Dict[str, Any]]:
    """
    Menerima dan mendekode satu paket pesan JSON utuh dari socket TCP.
    
    Langkah:
    1. Baca tepat 4 byte header untuk mengetahui panjang payload (N).
    2. Baca tepat N byte payload.
    3. Decode JSON UTF-8.
    
    Mengembalikan None jika koneksi ditutup secara normal oleh peer.
    """
    header_bytes = recv_exact(sock, HEADER_SIZE)
    if header_bytes is None:
        # Koneksi terputus normal pada boundary pesan
        return None

    # Ekstrak panjang payload
    (payload_length,) = struct.unpack(HEADER_FORMAT, header_bytes)

    if payload_length == 0:
        return {}

    # Baca payload tepat sebanyak payload_length byte
    payload_bytes = recv_exact(sock, payload_length)
    if payload_bytes is None:
        raise FramingError("Koneksi terputus saat membaca payload pesan.")

    try:
        decoded_str = payload_bytes.decode("utf-8")
        return json.loads(decoded_str)
    except UnicodeDecodeError as e:
        raise FramingError(f"Payload tidak dapat di-decode sebagai UTF-8: {e}")
    except json.JSONDecodeError as e:
        raise FramingError(f"Payload bukan format JSON yang valid: {e}")
