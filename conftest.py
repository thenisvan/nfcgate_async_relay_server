"""Shared pytest fixtures/helpers for the NFCGate relay server tests.

Tests launch the real `server.py` as a subprocess on an ephemeral port and talk
to it over raw sockets, exactly like the Android clients do.
"""
import os
import socket
import struct
import subprocess
import sys
import time

import pytest

REPO = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, REPO)  # make `server` and `plugins` importable

from plugins import c2c_pb2, c2s_pb2  # noqa: E402

READER = c2c_pb2.NFCData.READER
CARD = c2c_pb2.NFCData.CARD


# ---------------------------------------------------------------- wire helpers
def make_sd(apdu: bytes, source=READER) -> bytes:
    """Build a serialized ServerData wrapping one NFCData(apdu)."""
    nfc = c2c_pb2.NFCData()
    nfc.data_source = source
    nfc.data = apdu
    sd = c2s_pb2.ServerData()
    sd.data = nfc.SerializeToString()
    return sd.SerializeToString()


def send_frame(sock: socket.socket, session: int, sd_bytes: bytes) -> None:
    """client -> server framing: [uint32 len][uint8 session][ServerData]."""
    sock.sendall(struct.pack("!IB", len(sd_bytes), session) + sd_bytes)


def _recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("connection closed")
        buf += chunk
    return buf


def recv_frame(sock: socket.socket, timeout=3.0) -> bytes:
    """server -> client framing: [uint32 len][ServerData]. Returns ServerData bytes."""
    sock.settimeout(timeout)
    ln = struct.unpack("!I", _recv_exact(sock, 4))[0]
    return _recv_exact(sock, ln)


def apdu_of(sd_bytes: bytes):
    """Extract (source, apdu_hex) from a relayed ServerData."""
    sd = c2s_pb2.ServerData()
    sd.ParseFromString(sd_bytes)
    nfc = c2c_pb2.NFCData()
    nfc.ParseFromString(sd.data)
    return nfc.data_source, bytes(nfc.data).hex()


def drain(sock, t=0.3):
    sock.settimeout(t)
    try:
        while sock.recv(4096):
            pass
    except Exception:
        pass


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def _wait_listening(port, timeout=15.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def start_server(port, extra_args=None, metrics_port=0, health_port=0, log_port=0, env=None):
    """Launch server.py as a subprocess; returns the Popen handle."""
    args = [
        sys.executable, "server.py",
        "--port", str(port),
        "--health-port", str(health_port),
        "--metrics-port", str(metrics_port),
        "--log-port", str(log_port),
    ] + (extra_args or [])
    proc_env = dict(os.environ)
    if env:
        proc_env.update(env)
    proc = subprocess.Popen(args, cwd=REPO, env=proc_env)
    if not _wait_listening(port):
        proc.terminate()
        raise RuntimeError("server did not start listening in time")
    return proc


@pytest.fixture
def relay():
    """A running relay on an ephemeral port (extras/metrics/health/log off)."""
    port = free_port()
    proc = start_server(port)
    try:
        yield ("127.0.0.1", port)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def connect(addr):
    return socket.create_connection(addr, timeout=3)
