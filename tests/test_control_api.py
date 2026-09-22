"""Web control API: plugin list/toggle/config, delay, and live APDU rewrite."""
import json
import subprocess
import time
import urllib.request

from conftest import (CARD, READER, apdu_of, connect, drain, free_port,
                      make_sd, recv_frame, send_frame, start_server)


def _stop(proc):
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


def _api(lp, path, method="GET", body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"http://127.0.0.1:{lp}{path}", data=data, method=method)
    with urllib.request.urlopen(req, timeout=3) as r:
        return json.loads(r.read().decode())


def test_control_api_plugin_toggle_config_and_rewrite():
    port, lp = free_port(), free_port()
    while lp == port:
        lp = free_port()
    proc = start_server(port, log_port=lp)
    try:
        time.sleep(0.5)
        plugins = {p["name"]: p for p in _api(lp, "/api/plugins")["plugins"]}
        assert "modify" in plugins and plugins["modify"]["enabled"] is False

        _api(lp, "/api/plugins/modify/toggle", "POST", {"enabled": True})
        _api(lp, "/api/plugins/modify/config", "POST",
             {"rules": "a0000000041010=a0000000031010"})

        a, b = connect(("127.0.0.1", port)), connect(("127.0.0.1", port))
        send_frame(a, 1, make_sd(b"\x00", READER)); time.sleep(0.2)
        send_frame(b, 1, make_sd(b"\x00", CARD)); time.sleep(0.2)
        drain(a); drain(b)
        send_frame(a, 1, make_sd(bytes.fromhex("00a4040007a0000000041010"), READER))
        _, hexs = apdu_of(recv_frame(b))
        assert hexs == "00a4040007a0000000031010", f"expected portal rewrite, got {hexs}"
        a.close(); b.close()
    finally:
        _stop(proc)


def test_control_api_delay():
    port, lp = free_port(), free_port()
    while lp == port:
        lp = free_port()
    proc = start_server(port, log_port=lp)
    try:
        time.sleep(0.5)
        _api(lp, "/api/control/delay", "POST", {"ms": 120})
        assert _api(lp, "/api/control")["delay_ms"] == 120
    finally:
        _stop(proc)
