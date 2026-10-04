"""
Proof that the independent Parquet oracle detects real defects (Q02 / ISOL-04).

An oracle that has never been shown to fail is not evidence of anything. The
milestone requires demonstrating that the multiset comparison catches all three
corruption classes — a corrupted value, a removed duplicate, and a phantom row —
while preserving duplicate multiplicity.

A set comparison or a row count would pass all three of these defects; only a
full-row multiset catches them.
"""

import re
from pathlib import Path

import pytest

from tests.fixtures.deterministic_quotes import QuoteTick
from tests.support.lake_assertions import LAKE_COLUMNS, assert_row_multiset, row_multiset


def make_rows(count: int = 3) -> list[dict]:
    rows = []
    for i in range(count):
        tick = QuoteTick(
            timestamp=f"2026-10-02T14:30:00.{i:06d}",
            symbol="AAPL",
            price=150.25 + i,
            volume=100.0,
            bid=150.20 + i,
            ask=150.30 + i,
            source="CAPITAL",
            session="REG",
            ingest_id=f"oracle_{i:03d}",
        )
        rows.append(tick.__dict__.copy())
    return rows


def test_oracle_accepts_identical_rows():
    rows = make_rows()
    assert_row_multiset(rows, [dict(r) for r in rows])


def test_oracle_detects_corrupted_value():
    """One changed field must be caught, not absorbed by a row count check."""
    expected = make_rows()
    actual = make_rows()
    actual[1]["price"] = 999.99

    with pytest.raises(AssertionError):
        assert_row_multiset(actual, expected)


def test_oracle_detects_removed_duplicate():
    """Duplicate multiplicity is preserved, so dropping one twin must fail."""
    twin = make_rows(1)[0]
    expected = [dict(twin), dict(twin), make_rows(2)[1]]
    actual = [dict(twin), make_rows(2)[1]]

    # Sanity: the multiset really does carry a count of two for the twin row.
    # The key is derived through the oracle itself so timestamp normalization
    # is applied identically on both sides.
    twin_key = next(iter(row_multiset([twin])))
    assert row_multiset(expected)[twin_key] == 2

    with pytest.raises(AssertionError):
        assert_row_multiset(actual, expected)


def test_oracle_detects_phantom_row():
    expected = make_rows(2)
    actual = make_rows(2) + [make_rows(1)[0] | {"ingest_id": "phantom_001"}]

    with pytest.raises(AssertionError):
        assert_row_multiset(actual, expected)


def test_oracle_reports_missing_and_unexpected_separately():
    """The failure payload must distinguish what vanished from what appeared."""
    expected = make_rows(2)
    actual = [dict(expected[0])]

    with pytest.raises(AssertionError) as excinfo:
        assert_row_multiset(actual, expected)

    detail = excinfo.value.args[0]
    assert "missing" in detail and "unexpected" in detail
    assert detail["missing"], "expected the missing row to be reported"
    assert not detail["unexpected"], "no unexpected rows in this scenario"


def test_oracle_ignores_row_order():
    expected = make_rows(4)
    actual = list(reversed([dict(r) for r in expected]))
    assert_row_multiset(actual, expected)


def test_oracle_does_not_use_the_application_reader():
    """The oracle must stay independent of the code it is checking."""
    source = Path(__file__).resolve().parents[1] / "support" / "lake_assertions.py"
    text = source.read_text(encoding="utf-8")
    assert not re.search(r"from\s+src\.storage\.reader|import\s+reader", text), (
        "the oracle must not call the application reader"
    )
    assert "pyarrow" in text, "the oracle should read Parquet directly"
