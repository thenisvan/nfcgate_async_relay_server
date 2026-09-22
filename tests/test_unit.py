"""Pure unit tests for helpers that don't need a running server."""
import server
from conftest import CARD, READER, make_sd


def test_decode_frame_reader():
    sd = make_sd(bytes.fromhex("00a404000e325041592e5359532e4444463031"), READER)
    out = server.decode_frame(sd)
    assert out.get("src") == "READER"
    assert out.get("hex") == "00a404000e325041592e5359532e4444463031"
    assert "op" in out


def test_decode_frame_card():
    sd = make_sd(bytes.fromhex("6f5a8407a0000000041010889000"), CARD)
    out = server.decode_frame(sd)
    assert out.get("src") == "CARD"
    assert out.get("hex").endswith("9000")


def test_decode_frame_garbage_is_safe():
    # non-protobuf bytes must not raise; returns a dict (possibly empty)
    out = server.decode_frame(b"\xff\xff\xff\xff")
    assert isinstance(out, dict)


def test_weblog_page_loaded():
    assert "NFCGate relay" in server.LOG_PAGE


def test_decode_frame_dtp_initial():
    from conftest import READER
    sd = make_sd(bytes.fromhex("00a404000e325041592e5359532e4444463031"), READER)
    out = server.decode_frame(sd)
    assert out.get("dtp") == "INITIAL"  # make_sd leaves default data_type = INITIAL


def test_decode_frame_dtp_continuation():
    import conftest
    nfc = conftest.c2c_pb2.NFCData()
    nfc.data_source = conftest.READER
    nfc.data_type = conftest.c2c_pb2.NFCData.CONTINUATION
    nfc.data = b"\x00\xa4"
    sd = conftest.c2s_pb2.ServerData()
    sd.data = nfc.SerializeToString()
    out = server.decode_frame(sd.SerializeToString())
    assert out.get("dtp") == "CONT"
