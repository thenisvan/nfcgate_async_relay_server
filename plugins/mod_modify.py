"""mod_modify — lab-only on-the-fly APDU rewriter for the NFCGate relay.

Demonstrates that a relay MITM can not only observe but ALTER a transaction.
Inspired by penegui/MITM-PixNFC's `mod_pixpatch`, but reimplemented against this
server's protobuf frame format (ServerData -> NFCData -> APDU) instead of doing a
byte-search on the wire buffer, so it is protocol-correct and robust.

SAFETY
------
Disabled unless rules are configured, so loading the plugin alone changes nothing.
Configure rules via environment variable:

    NFCGATE_PATCH_RULES="<find_hex>=<replace_hex>[,<find_hex>=<replace_hex>...]"

Each rule replaces the FIRST occurrence of find_hex with replace_hex inside the
relayed APDU. Same-length replacements are recommended and applied as-is;
different lengths are still applied but flagged, because they can break APDU
framing (Lc / status word). Use only on your own or test cards, in a lab.

Enable with:  python server.py modify        (module mod_modify -> plugin "modify")
"""
import os

from plugins import c2c_pb2, c2s_pb2


def _parse_rules(spec):
    rules = []
    for part in (spec or "").split(","):
        part = part.strip()
        if not part or "=" not in part:
            continue
        find_s, repl_s = part.split("=", 1)
        try:
            fb = bytes.fromhex(find_s.strip())
            rb = bytes.fromhex(repl_s.strip())
        except ValueError:
            continue
        if fb:
            rules.append((fb, rb))
    return rules


RULES = _parse_rules(os.environ.get("NFCGATE_PATCH_RULES", ""))


async def handle_data(log, data, state):
    # Disabled (no rules) -> transparent passthrough.
    if not RULES:
        return data
    try:
        sd = c2s_pb2.ServerData()
        sd.ParseFromString(data)
        if not sd.data:
            return data
        nfc = c2c_pb2.NFCData()
        nfc.ParseFromString(sd.data)
        apdu = bytes(nfc.data)
        patched = apdu
        for fb, rb in RULES:
            if fb in patched:
                patched = patched.replace(fb, rb, 1)
                if len(rb) != len(fb):
                    log(f"[PATCH] length change {len(fb)}->{len(rb)} bytes may break APDU framing",
                        level="WARNING")
        if patched == apdu:
            return data
        src = "CARD" if nfc.data_source == c2c_pb2.NFCData.CARD else "READER"
        log(f"[PATCH] {src} APDU rewritten: {apdu.hex()} -> {patched.hex()}", level="WARNING")
        nfc.data = patched
        sd.data = nfc.SerializeToString()
        return sd.SerializeToString()
    except Exception as e:
        # Never corrupt the relay: on any error pass the original frame through.
        log(f"[PATCH] error, passing through: {e}", level="ERROR")
        return data
