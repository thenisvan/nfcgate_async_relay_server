"""Operational features: Prometheus gauge balance and the IP allowlist."""
import socket
import subprocess
import time
import urllib.request

import pytest

from conftest import (CARD, READER, apdu_of, connect, drain, free_port,
                      make_sd, recv_frame, send_frame, start_server)

SELECT_PPSE = bytes.fromhex("00a404000e325041592e5359532e4444463031")


def _stop(proc):
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


def _scrape(mport, name="active_connections", retries=10):
    for _ in range(retries):
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{mport}/", timeout=2) as r:
                for line in r.read().decode().splitlines():
                    if line.startswith(name):
                        return float(line.split()[1])
        except Exception:
            time.sleep(0.2)
    raise AssertionError("metric not scraped")


def test_gauge_balances_and_never_negative():
    port = free_port()
    mport = free_port()
    while mport == port:
        mport = free_port()
    proc = start_server(port, metrics_port=mport)
    try:
        assert _scrape(mport) == 0.0
        a, b = connect(("127.0.0.1", port)), connect(("127.0.0.1", port))
        time.sleep(0.4)
        assert _scrape(mport) == 2.0
        a.close()
        b.close()
        time.sleep(0.6)
        assert _scrape(mport) == 0.0
    finally:
        _stop(proc)


def test_allowlist_rejects_outsider():
    """allowlist excluding localhost => server closes our connection immediately."""
    port = free_port()
    proc = start_server(port, extra_args=["--allow", "10.0.0.0/8"])
    try:
        s = socket.create_connection(("127.0.0.1", port), timeout=3)
        s.settimeout(2.0)
        # server accepts the TCP connection then closes it -> EOF
        assert s.recv(16) == b""
        s.close()
    finally:
        _stop(proc)


def test_allowlist_allows_listed():
    port = free_port()
    proc = start_server(port, extra_args=["--allow", "127.0.0.1/32,10.0.0.0/8"])
    try:
        a, b = connect(("127.0.0.1", port)), connect(("127.0.0.1", port))
        send_frame(a, 5, make_sd(b"\x00", READER))
        time.sleep(0.2)
        send_frame(b, 5, make_sd(b"\x00", CARD))
        time.sleep(0.2)
        drain(a)
        drain(b)
        send_frame(a, 5, make_sd(SELECT_PPSE, READER))
        _, hexs = apdu_of(recv_frame(b))
        assert hexs == SELECT_PPSE.hex()
        a.close()
        b.close()
    finally:
        _stop(proc)
