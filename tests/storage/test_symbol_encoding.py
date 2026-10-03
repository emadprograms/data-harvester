"""
Unit tests for symbol encoding, decoding, partition path assembly,
and path traversal security guards (Milestone v4.0 - Phase 16).
"""
from datetime import date, datetime
from pathlib import Path
import pytest

from src.storage.config import (
    PathTraversalError,
    encode_symbol,
    decode_symbol,
    get_partition_path,
)


def test_standard_symbols_identity():
    """
    Standard alphanumeric symbols (including '_' and '-') must remain unchanged.
    """
    standard_symbols = [
        "AAPL",
        "NVDA",
        "MSFT",
        "SPY",
        "QQQ",
        "GOOG_L",
        "BTC-USD",
        "TSLA123",
    ]

    for sym in standard_symbols:
        encoded = encode_symbol(sym)
        assert encoded == sym, f"Expected {sym} to remain unchanged, got {encoded}"
        decoded = decode_symbol(encoded)
        assert decoded == sym, f"Expected {encoded} to decode back to {sym}, got {decoded}"


def test_special_symbol_roundtrip():
    """
    Special characters (., /, =, :, @, ^) must be safely percent-encoded using
    uppercase hexadecimal and reliably round-trip back to their original form.
    """
    test_cases = [
        ("BRK.A", "BRK%2EA"),
        ("BTC/USDT", "BTC%2FUSDT"),
        ("EUR/USD", "EUR%2FUSD"),
        ("BF=B", "BF%3DB"),
        ("BINANCE:BTCUSDT", "BINANCE%3ABTCUSDT"),
        ("@ES", "%40ES"),
        ("^GSPC", "%5EGSPC"),
    ]

    for raw, expected_encoded in test_cases:
        encoded = encode_symbol(raw)
        assert encoded == expected_encoded, (
            f"Encoding {raw}: expected {expected_encoded}, got {encoded}"
        )
        decoded = decode_symbol(encoded)
        assert decoded == raw, (
            f"Decoding {encoded}: expected {raw}, got {decoded}"
        )


def test_path_traversal_rejection():
    """
    Verify that directory traversal sequences, null bytes, backslashes,
    empty strings, and excessively long symbols are strictly rejected.
    """
    malicious_symbols = [
        "..",
        ".",
        "../foo",
        "foo/../../bar",
        "\\..\\",
        "foo\\bar",
        "AAPL\x00",
        "\x00NVDA",
        "",
        "A" * 65,  # Exceeds max 64 chars
    ]

    for bad_sym in malicious_symbols:
        with pytest.raises((ValueError, PathTraversalError)):
            encode_symbol(bad_sym)

    # Test non-canonical and malicious decode attempts
    malicious_decodes = [
        "%2e%2e",  # Traversal '..' encoded
        "%2E%2E",
        "%00",      # Null byte encoded
        "%ZZ",      # Malformed hex
        "",
    ]
    for bad_encoded in malicious_decodes:
        with pytest.raises((ValueError, PathTraversalError)):
            decode_symbol(bad_encoded)


def test_get_partition_path_containment(tmp_path):
    """
    Verify get_partition_path produces a path strictly contained within root/ticks,
    formats date correctly from datetime/date/string, and does not create accidental
    nested directories for slashed symbols.
    """
    lake_root = (tmp_path / "lake").resolve()
    ticks_dir = (lake_root / "ticks").resolve()

    # 1. Standard symbol with datetime
    p1 = get_partition_path(lake_root, "AAPL", datetime(2026, 10, 2, 14, 30, 0))
    expected_p1 = (ticks_dir / "symbol=AAPL" / "date=2026-10-02").resolve()
    assert p1 == expected_p1
    assert p1.is_relative_to(ticks_dir)

    # 2. Standard symbol with date object
    p2 = get_partition_path(lake_root, "AAPL", date(2026, 10, 2))
    assert p2 == expected_p1
    assert p2.is_relative_to(ticks_dir)

    # 3. Standard symbol with YYYY-MM-DD string
    p3 = get_partition_path(lake_root, "AAPL", "2026-10-02")
    assert p3 == expected_p1
    assert p3.is_relative_to(ticks_dir)

    # 4. Slashed symbol must produce exactly 2 levels of hierarchy under ticks/
    p_slash = get_partition_path(lake_root, "BTC/USDT", "2026-10-02")
    expected_slash = (ticks_dir / "symbol=BTC%2FUSDT" / "date=2026-10-02").resolve()
    assert p_slash == expected_slash
    assert p_slash.is_relative_to(ticks_dir)
    # Check directory hierarchy depth: ticks -> symbol=... -> date=... (exactly 2 child levels)
    rel = p_slash.relative_to(ticks_dir)
    assert len(rel.parts) == 2
    assert rel.parts[0] == "symbol=BTC%2FUSDT"
    assert rel.parts[1] == "date=2026-10-02"

    # 5. Dotted symbol
    p_dot = get_partition_path(lake_root, "BRK.A", "2026-10-02")
    expected_dot = (ticks_dir / "symbol=BRK%2EA" / "date=2026-10-02").resolve()
    assert p_dot == expected_dot
    assert p_dot.is_relative_to(ticks_dir)

    # 6. Malicious symbol or date traversal attempts
    with pytest.raises((ValueError, PathTraversalError)):
        get_partition_path(lake_root, "../escape", "2026-10-02")

    with pytest.raises((ValueError, PathTraversalError)):
        get_partition_path(lake_root, "AAPL", "../../escape")
