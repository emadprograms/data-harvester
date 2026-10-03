"""
Unit tests for Lake Schema v1 definition, PyArrow table builders,
and vectorized validation constraints (Milestone v4.0 - Phase 16).
"""
from datetime import datetime, timezone
import math
import pytest
import pyarrow as pa

from src.storage.schema import (
    SchemaValidationError,
    SCHEMA_V1_VERSION,
    SCHEMA_V1_VERSION_STR,
    SCHEMA_V1_METADATA_KEY,
    SCHEMA_V1_FORMAT_KEY,
    SCHEMA_V1_FORMAT_VAL,
    SCHEMA_V1_COLUMNS,
    LAKE_SCHEMA_V1,
    validate_schema_v1,
    validate_table_v1,
    ticks_to_table,
    table_to_ticks,
)
from tests.fixtures.deterministic_quotes import (
    QuoteTick,
    generate_subsecond_burst,
    generate_null_and_capital_quotes,
    generate_exact_duplicates,
)


def test_schema_v1_field_types_and_metadata():
    """
    Verify LAKE_SCHEMA_V1 definition:
    - Exactly 9 columns in canonical order
    - Correct data types (timestamp[us], string, double)
    - Nullability constraints (timestamp, symbol, price, ingest_id non-nullable)
    - Metadata tags (schema_version='1', format='tick_lake_v1')
    """
    assert LAKE_SCHEMA_V1 is not None, "LAKE_SCHEMA_V1 must be defined"
    assert len(LAKE_SCHEMA_V1) == 9
    assert LAKE_SCHEMA_V1.names == SCHEMA_V1_COLUMNS

    expected_types_and_nullability = {
        "timestamp": (pa.timestamp("us"), False),
        "symbol": (pa.string(), False),
        "price": (pa.float64(), False),
        "volume": (pa.float64(), True),
        "bid": (pa.float64(), True),
        "ask": (pa.float64(), True),
        "source": (pa.string(), True),
        "session": (pa.string(), True),
        "ingest_id": (pa.string(), False),
    }

    for name, (expected_type, expected_nullable) in expected_types_and_nullability.items():
        field = LAKE_SCHEMA_V1.field(name)
        assert field.type == expected_type, f"Field {name} type mismatch: expected {expected_type}, got {field.type}"
        assert field.nullable == expected_nullable, (
            f"Field {name} nullable mismatch: expected {expected_nullable}, got {field.nullable}"
        )

    # Verify metadata
    metadata = LAKE_SCHEMA_V1.metadata
    assert metadata is not None, "LAKE_SCHEMA_V1 missing metadata"
    assert metadata.get(SCHEMA_V1_METADATA_KEY) == SCHEMA_V1_VERSION_STR.encode("utf-8")
    assert metadata.get(SCHEMA_V1_FORMAT_KEY) == SCHEMA_V1_FORMAT_VAL

    assert validate_schema_v1(LAKE_SCHEMA_V1) is True


def test_ticks_to_table_from_quote_ticks():
    """
    Verify ticks_to_table properly converts QuoteTick instances to a PyArrow Table
    with matching schema, correct column data, and row count.
    """
    burst_ticks = generate_subsecond_burst("NVDA", count=25)
    table = ticks_to_table(burst_ticks, validate=True)

    assert isinstance(table, pa.Table)
    assert table.num_rows == 25
    assert table.column_names == SCHEMA_V1_COLUMNS
    assert table.schema == LAKE_SCHEMA_V1

    # Check first row values
    first = burst_ticks[0]
    assert table["symbol"][0].as_py() == first.symbol
    assert table["price"][0].as_py() == pytest.approx(first.price)
    assert table["ingest_id"][0].as_py() == first.ingest_id
    assert validate_table_v1(table) is True


def test_ticks_to_table_from_dicts_and_tuples():
    """
    Verify ticks_to_table can ingest row dictionaries and tuples,
    handling ISO string timestamps and timezone conversion to UTC-naive.
    """
    # 1. Test dictionary records with mixed timestamp representations
    dict_records = [
        {
            "timestamp": "2026-10-02T14:30:00.123456",
            "symbol": "AAPL",
            "price": 150.25,
            "volume": 100.0,
            "bid": 150.20,
            "ask": 150.30,
            "source": "CAPITAL",
            "session": "REG",
            "ingest_id": "dict_01",
        },
        {
            # Timezone-aware timestamp (EDT: UTC-4 -> should become 18:30:00.500000 UTC)
            "timestamp": datetime(2026, 10, 2, 14, 30, 0, 500000, tzinfo=timezone.utc),
            "symbol": "AAPL",
            "price": 150.50,
            "volume": 200.0,
            "bid": 150.45,
            "ask": 150.55,
            "source": "CAPITAL",
            "session": "REG",
            "ingest_id": "dict_02",
        },
    ]

    table_dict = ticks_to_table(dict_records, validate=True)
    assert table_dict.num_rows == 2
    assert table_dict["timestamp"].type == pa.timestamp("us")
    assert table_dict["symbol"][0].as_py() == "AAPL"
    assert table_dict["ingest_id"][1].as_py() == "dict_02"

    # 2. Test tuple records
    tuple_records = [
        (
            datetime(2026, 10, 2, 14, 30, 0, 100000),
            "MSFT",
            420.50,
            50.0,
            420.40,
            420.60,
            "CAPITAL",
            "REG",
            "tup_01",
        )
    ]
    table_tuple = ticks_to_table(tuple_records, validate=True)
    assert table_tuple.num_rows == 1
    assert table_tuple["symbol"][0].as_py() == "MSFT"
    assert table_tuple["price"][0].as_py() == pytest.approx(420.50)


def test_validation_rejects_nan_and_inf_price():
    """
    Vectorized validation must reject tables containing NaN, +Inf, or -Inf in price.
    """
    # 1. Test NaN
    bad_ticks_nan = [
        QuoteTick(
            timestamp=datetime(2026, 10, 2, 14, 30, 0),
            symbol="AAPL",
            price=float("nan"),
            ingest_id="nan_price",
        )
    ]
    with pytest.raises(SchemaValidationError):
        ticks_to_table(bad_ticks_nan, validate=True)

    # 2. Test +Inf
    bad_ticks_inf = [
        QuoteTick(
            timestamp=datetime(2026, 10, 2, 14, 30, 0),
            symbol="AAPL",
            price=float("inf"),
            ingest_id="inf_price",
        )
    ]
    with pytest.raises(SchemaValidationError):
        ticks_to_table(bad_ticks_inf, validate=True)

    # 3. Test -Inf
    bad_ticks_neginf = [
        QuoteTick(
            timestamp=datetime(2026, 10, 2, 14, 30, 0),
            symbol="AAPL",
            price=float("-inf"),
            ingest_id="neginf_price",
        )
    ]
    with pytest.raises(SchemaValidationError):
        ticks_to_table(bad_ticks_neginf, validate=True)


def test_validation_rejects_negative_or_zero_price():
    """
    Vectorized validation must reject non-positive prices (price <= 0.0).
    """
    # 1. Zero price
    zero_price_tick = [
        QuoteTick(
            timestamp=datetime(2026, 10, 2, 14, 30, 0),
            symbol="AAPL",
            price=0.0,
            ingest_id="zero_price",
        )
    ]
    with pytest.raises(SchemaValidationError):
        ticks_to_table(zero_price_tick, validate=True)

    # 2. Negative price
    neg_price_tick = [
        QuoteTick(
            timestamp=datetime(2026, 10, 2, 14, 30, 0),
            symbol="AAPL",
            price=-10.50,
            ingest_id="neg_price",
        )
    ]
    with pytest.raises(SchemaValidationError):
        ticks_to_table(neg_price_tick, validate=True)


def test_validation_accepts_nullable_fields():
    """
    Verify validation accepts None in volume, bid, ask, source, session,
    but rejects negative values in volume, bid, or ask if provided.
    """
    null_quotes = generate_null_and_capital_quotes("AAPL")
    table = ticks_to_table(null_quotes, validate=True)
    assert validate_table_v1(table) is True

    # Reject negative volume
    bad_vol_tick = [
        QuoteTick(
            timestamp=datetime(2026, 10, 2, 14, 30, 0),
            symbol="AAPL",
            price=150.0,
            volume=-1.0,
            ingest_id="neg_vol",
        )
    ]
    with pytest.raises(SchemaValidationError):
        ticks_to_table(bad_vol_tick, validate=True)

    # Reject negative bid
    bad_bid_tick = [
        QuoteTick(
            timestamp=datetime(2026, 10, 2, 14, 30, 0),
            symbol="AAPL",
            price=150.0,
            bid=-0.5,
            ingest_id="neg_bid",
        )
    ]
    with pytest.raises(SchemaValidationError):
        ticks_to_table(bad_bid_tick, validate=True)


def test_roundtrip_table_to_ticks():
    """
    Verify lossless round-trip: table_to_ticks(ticks_to_table(quotes)) == quotes.
    """
    original_quotes = generate_null_and_capital_quotes("AAPL") + generate_exact_duplicates("NVDA")
    table = ticks_to_table(original_quotes, validate=True)
    roundtripped = table_to_ticks(table)

    assert len(roundtripped) == len(original_quotes)

    for orig, rt in zip(original_quotes, roundtripped):
        assert isinstance(rt, QuoteTick)
        assert orig.timestamp == rt.timestamp
        assert orig.symbol == rt.symbol
        assert orig.price == pytest.approx(rt.price)
        if orig.volume is None:
            assert rt.volume is None
        else:
            assert orig.volume == pytest.approx(rt.volume)
        if orig.bid is None:
            assert rt.bid is None
        else:
            assert orig.bid == pytest.approx(rt.bid)
        if orig.ask is None:
            assert rt.ask is None
        else:
            assert orig.ask == pytest.approx(rt.ask)
        assert orig.source == rt.source
        assert orig.session == rt.session
        assert orig.ingest_id == rt.ingest_id
