"""Relay behaviour: bidirectional relay, session isolation, peer limit, reconnect."""
import time

from conftest import (CARD, READER, apdu_of, connect, drain, make_sd,
                      recv_frame, send_frame)

SELECT_PPSE = bytes.fromhex("00a404000e325041592e5359532e4444463031")
RESP_9000 = bytes.fromhex("6f5a8407a0000000041010889000")


def _join(sock, session, source):
    send_frame(sock, session, make_sd(b"\x00", source))
    time.sleep(0.2)


def test_relay_bidirectional(relay):
    a, b = connect(relay), connect(relay)
    try:
        _join(a, 7, READER)
        _join(b, 7, CARD)
        drain(a)
        drain(b)

        send_frame(a, 7, make_sd(SELECT_PPSE, READER))
        src, hexs = apdu_of(recv_frame(b))
        assert hexs == SELECT_PPSE.hex()
        assert src == READER

        send_frame(b, 7, make_sd(RESP_9000, CARD))
        src2, hexs2 = apdu_of(recv_frame(a))
        assert hexs2 == RESP_9000.hex()
        assert src2 == CARD
    finally:
        a.close()
        b.close()


def test_session_isolation(relay):
    """A frame in session 1 must not reach a client in session 2."""
    a, b = connect(relay), connect(relay)
    try:
        _join(a, 1, READER)
        _join(b, 2, CARD)
        drain(a)
        drain(b)

        send_frame(a, 1, make_sd(SELECT_PPSE, READER))
        b.settimeout(0.6)
        try:
            data = b.recv(4096)
        except Exception:
            data = b""
        assert data == b"", "client in a different session received the frame"
    finally:
        a.close()
        b.close()


def test_peer_limit_rejects_third(relay):
    """With max-peers=2 (default), a 3rd client's frames are not relayed."""
    a, b, c = connect(relay), connect(relay), connect(relay)
    try:
        _join(a, 9, READER)
        _join(b, 9, CARD)
        _join(c, 9, READER)  # rejected: session already has 2
        drain(a)
        drain(b)

        marker = bytes.fromhex("deadbeefcafe")
        send_frame(c, 9, make_sd(marker, READER))
        time.sleep(0.4)

        for peer in (a, b):
            peer.settimeout(0.5)
            got = b""
            try:
                got = peer.recv(4096)
            except Exception:
                got = b""
            assert marker.hex().encode() not in got.hex().encode()
    finally:
        a.close()
        b.close()
        c.close()


def test_reconnect_after_peer_leaves(relay):
    """After one peer disconnects, a fresh peer can join and relay works."""
    a, b = connect(relay), connect(relay)
    _join(a, 3, READER)
    _join(b, 3, CARD)
    drain(a)
    b.close()
    time.sleep(0.3)

    c = connect(relay)
    try:
        _join(c, 3, CARD)
        drain(a)
        drain(c)
        send_frame(a, 3, make_sd(SELECT_PPSE, READER))
        _, hexs = apdu_of(recv_frame(c))
        assert hexs == SELECT_PPSE.hex()
    finally:
        a.close()
        c.close()
