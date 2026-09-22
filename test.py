#!/usr/bin/env python3
"""Smoke test for the async NFCGate relay server (v2 protocol).

Connects two clients to one session and verifies a frame is relayed both ways.
Wire framing:  client->server  [uint32 len][uint8 session][ServerData]
               server->client  [uint32 len][ServerData]   (session byte omitted, by design)

Usage:  python test.py [host] [port]   (defaults 127.0.0.1 5566)
Exit code 0 on success.
"""
import socket
import struct
import sys
import time

from plugins import c2c_pb2, c2s_pb2

HOST = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 5566
SESSION = 77


def frame(server_data_bytes):
    return struct.pack("!IB", len(server_data_bytes), SESSION) + server_data_bytes


def make(apdu_bytes, source):
    nfc = c2c_pb2.NFCData()
    nfc.data_source = source
    nfc.data = apdu_bytes
    sd = c2s_pb2.ServerData()
    sd.data = nfc.SerializeToString()
    return sd.SerializeToString()


def drain(sock, t=0.25):
    sock.settimeout(t)
    try:
        while sock.recv(4096):
            pass
    except Exception:
        pass


def recv_one(sock, t=3.0):
    sock.settimeout(t)
    hdr = b""
    while len(hdr) < 4:
        hdr += sock.recv(4 - len(hdr))
    ln = struct.unpack("!I", hdr)[0]
    buf = b""
    while len(buf) < ln:
        buf += sock.recv(ln - len(buf))
    sd = c2s_pb2.ServerData()
    sd.ParseFromString(buf)
    nfc = c2c_pb2.NFCData()
    nfc.ParseFromString(sd.data)
    return nfc


def main():
    a = socket.create_connection((HOST, PORT))
    a.sendall(frame(make(b"\x00", c2c_pb2.NFCData.READER)))
    time.sleep(0.3)
    b = socket.create_connection((HOST, PORT))
    b.sendall(frame(make(b"\x00", c2c_pb2.NFCData.CARD)))
    time.sleep(0.3)
    drain(a)
    drain(b)

    # READER -> CARD : SELECT PPSE
    select_ppse = bytes.fromhex("00a404000e325041592e5359532e4444463031")
    t0 = time.perf_counter()
    a.sendall(frame(make(select_ppse, c2c_pb2.NFCData.READER)))
    got = recv_one(b)
    rtt = (time.perf_counter() - t0) * 1000
    assert got.data.hex() == select_ppse.hex(), "A->B payload mismatch"
    print(f"[OK] A->B relay (SELECT PPSE), RTT ~{rtt:.1f} ms")

    # CARD -> READER : response 9000
    resp = bytes.fromhex("6f5a8407a0000000041010889000")
    b.sendall(frame(make(resp, c2c_pb2.NFCData.CARD)))
    got2 = recv_one(a)
    assert got2.data.hex() == resp.hex(), "B->A payload mismatch"
    print("[OK] B->A relay (response 9000)")

    a.close()
    b.close()
    print("SMOKE TEST PASSED")


if __name__ == "__main__":
    try:
        main()
    except AssertionError as e:
        print(f"[FAIL] {e}")
        sys.exit(1)
    except OSError as e:
        print(f"[FAIL] cannot reach relay at {HOST}:{PORT}: {e}")
        sys.exit(2)
