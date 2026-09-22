"""mod_modify plugin: on-the-fly APDU rewrite (lab MITM demo)."""
import subprocess
import time

from conftest import (CARD, READER, apdu_of, connect, drain, free_port,
                      make_sd, recv_frame, send_frame, start_server)


def _stop(proc):
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


def test_modify_rewrites_apdu():
    """With a patch rule loaded, the relayed APDU is rewritten in flight."""
    port = free_port()
    # replace the AID a0000000041010 (Mastercard) with a0000000031010 (Visa)
    rule = "a0000000041010=a0000000031010"
    proc = start_server(port, extra_args=["modify"],
                        env={"NFCGATE_PATCH_RULES": rule})
    try:
        a, b = connect(("127.0.0.1", port)), connect(("127.0.0.1", port))
        send_frame(a, 1, make_sd(b"\x00", READER))
        time.sleep(0.2)
        send_frame(b, 1, make_sd(b"\x00", CARD))
        time.sleep(0.2)
        drain(a)
        drain(b)

        original = bytes.fromhex("00a4040007a0000000041010")   # SELECT AID (Mastercard)
        expected = bytes.fromhex("00a4040007a0000000031010")   # rewritten to Visa AID
        send_frame(a, 1, make_sd(original, READER))
        _, hexs = apdu_of(recv_frame(b))
        assert hexs == expected.hex(), f"expected rewrite, got {hexs}"
    finally:
        a.close()
        b.close()
        _stop(proc)


def test_modify_passthrough_without_rules():
    """No rules -> plugin is a transparent passthrough."""
    port = free_port()
    proc = start_server(port, extra_args=["modify"])  # no NFCGATE_PATCH_RULES
    try:
        a, b = connect(("127.0.0.1", port)), connect(("127.0.0.1", port))
        send_frame(a, 2, make_sd(b"\x00", READER))
        time.sleep(0.2)
        send_frame(b, 2, make_sd(b"\x00", CARD))
        time.sleep(0.2)
        drain(a)
        drain(b)

        original = bytes.fromhex("00a4040007a0000000041010")
        send_frame(a, 2, make_sd(original, READER))
        _, hexs = apdu_of(recv_frame(b))
        assert hexs == original.hex()
    finally:
        a.close()
        b.close()
        _stop(proc)
