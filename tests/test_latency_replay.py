"""Artificial relay latency, frame recording, and capture replay."""
import json
import os
import subprocess
import time
import urllib.request

from conftest import (CARD, READER, apdu_of, connect, drain, free_port,
                      make_sd, recv_frame, send_frame, start_server)

SELECT_PPSE = bytes.fromhex("00a404000e325041592e5359532e4444463031")
RESP_9000 = bytes.fromhex("6f5a8407a0000000041010889000")


def _stop(proc):
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


def _join(sock, session, source):
    send_frame(sock, session, make_sd(b"\x00", source))
    time.sleep(0.2)


def test_delay_adds_latency():
    port = free_port()
    proc = start_server(port, extra_args=["--delay-ms", "150"])
    try:
        a, b = connect(("127.0.0.1", port)), connect(("127.0.0.1", port))
        _join(a, 1, READER)
        _join(b, 1, CARD)
        drain(a)
        drain(b)
        t0 = time.perf_counter()
        send_frame(a, 1, make_sd(SELECT_PPSE, READER))
        recv_frame(b)
        elapsed = time.perf_counter() - t0
        assert elapsed >= 0.13, f"expected >=150ms relay delay, got {elapsed*1000:.0f}ms"
    finally:
        a.close()
        b.close()
        _stop(proc)


def test_record_writes_jsonl(tmp_path):
    port = free_port()
    rec = str(tmp_path / "capture.jsonl")
    proc = start_server(port, extra_args=["--record", rec])
    try:
        a, b = connect(("127.0.0.1", port)), connect(("127.0.0.1", port))
        _join(a, 1, READER)
        _join(b, 1, CARD)
        drain(a)
        drain(b)
        send_frame(a, 1, make_sd(SELECT_PPSE, READER))
        recv_frame(b)
        time.sleep(0.2)
    finally:
        a.close()
        b.close()
        _stop(proc)

    assert os.path.exists(rec)
    events = [json.loads(l) for l in open(rec) if l.strip()]
    assert events, "no frames recorded"
    assert all("ms" in e for e in events)
    assert any(e.get("hex") == SELECT_PPSE.hex() for e in events)


def test_replay_into_log(tmp_path):
    # Craft a small capture and replay it into the web log (no clients).
    cap = tmp_path / "demo.jsonl"
    base = int(time.time() * 1000)
    frames = [
        {"t": "frame", "ms": base, "session": 1, "src": "READER", "hex": SELECT_PPSE.hex()},
        {"t": "frame", "ms": base + 50, "session": 1, "src": "CARD", "hex": RESP_9000.hex()},
    ]
    cap.write_text("\n".join(json.dumps(f) for f in frames) + "\n")

    port, lp = free_port(), free_port()
    while lp == port:
        lp = free_port()
    proc = start_server(port, extra_args=["--replay", str(cap)], log_port=lp)
    try:
        time.sleep(1.0)  # allow replay to publish
        with urllib.request.urlopen(f"http://127.0.0.1:{lp}/logs", timeout=3) as r:
            logged = json.loads(r.read().decode())
        replayed = [e for e in logged if e.get("t") == "frame" and e.get("replay")]
        assert replayed, "no replayed frames in the log"
        hexes = {e.get("hex") for e in replayed}
        assert SELECT_PPSE.hex() in hexes and RESP_9000.hex() in hexes
    finally:
        _stop(proc)
