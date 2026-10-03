"""
Lake Schema v1 definition, PyArrow builders, and vectorized validations.
"""
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
import pyarrow as pa
import pyarrow.compute as pc

try:
    from tests.fixtures.deterministic_quotes import QuoteTick
except (ImportError, ModuleNotFoundError):
    @dataclass
    class QuoteTick:
        """Physical representation of a raw quote tick adhering strictly to Lake Schema v1."""
        timestamp: datetime
        symbol: str
        price: float
        volume: Optional[float] = None
        bid: Optional[float] = None
        ask: Optional[float] = None
        source: Optional[str] = "CAPITAL"
        session: Optional[str] = "REG"
        ingest_id: str = ""

        def __post_init__(self):
            if isinstance(self.timestamp, str):
                self.timestamp = datetime.fromisoformat(self.timestamp)
            if self.timestamp.tzinfo is not None:
                self.timestamp = self.timestamp.astimezone(timezone.utc).replace(tzinfo=None)
            self.price = float(self.price)
            if self.volume is not None:
                self.volume = float(self.volume)
            if self.bid is not None:
                self.bid = float(self.bid)
            if self.ask is not None:
                self.ask = float(self.ask)
            if not self.ingest_id:
                ts_micro = int(self.timestamp.replace(tzinfo=timezone.utc).timestamp() * 1_000_000)
                self.ingest_id = f"gen_{self.symbol}_{ts_micro}"

        def to_dict(self) -> Dict[str, Any]:
            return {
                "timestamp": self.timestamp,
                "symbol": self.symbol,
                "price": self.price,
                "volume": self.volume,
                "bid": self.bid,
                "ask": self.ask,
                "source": self.source,
                "session": self.session,
                "ingest_id": self.ingest_id,
            }

        def to_tuple(self) -> Tuple:
            return (
                self.timestamp,
                self.symbol,
                self.price,
                self.volume,
                self.bid,
                self.ask,
                self.source,
                self.session,
                self.ingest_id,
            )

        def __getitem__(self, item: str) -> Any:
            return getattr(self, item)


SCHEMA_V1_VERSION = 1
SCHEMA_V1_VERSION_STR = "1"
SCHEMA_V1_METADATA_KEY = b"schema_version"
SCHEMA_V1_FORMAT_KEY = b"format"
SCHEMA_V1_FORMAT_VAL = b"tick_lake_v1"

SCHEMA_V1_COLUMNS = [
    "timestamp",
    "symbol",
    "price",
    "volume",
    "bid",
    "ask",
    "source",
    "session",
    "ingest_id",
]

LAKE_SCHEMA_V1 = pa.schema(
    [
        pa.field("timestamp", pa.timestamp("us"), nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("price", pa.float64(), nullable=False),
        pa.field("volume", pa.float64(), nullable=True),
        pa.field("bid", pa.float64(), nullable=True),
        pa.field("ask", pa.float64(), nullable=True),
        pa.field("source", pa.string(), nullable=True),
        pa.field("session", pa.string(), nullable=True),
        pa.field("ingest_id", pa.string(), nullable=False),
    ],
    metadata={
        SCHEMA_V1_METADATA_KEY: SCHEMA_V1_VERSION_STR.encode("utf-8"),
        SCHEMA_V1_FORMAT_KEY: SCHEMA_V1_FORMAT_VAL,
    },
)


class SchemaValidationError(ValueError):
    """Raised when data or schema violates Lake Schema v1 constraints."""
    pass


def validate_schema_v1(schema_or_table: Union[pa.Schema, pa.Table, pa.RecordBatch]) -> bool:
    """Validate that schema matches Lake Schema v1 exact column names, order, types, and nullability."""
    if isinstance(schema_or_table, (pa.Table, pa.RecordBatch)):
        schema = schema_or_table.schema
    elif isinstance(schema_or_table, pa.Schema):
        schema = schema_or_table
    else:
        raise SchemaValidationError(f"Expected Schema, Table, or RecordBatch, got {type(schema_or_table)}")

    if len(schema) != len(SCHEMA_V1_COLUMNS):
        raise SchemaValidationError(
            f"Schema column count mismatch: expected {len(SCHEMA_V1_COLUMNS)}, got {len(schema)}"
        )

    if schema.names != SCHEMA_V1_COLUMNS:
        raise SchemaValidationError(
            f"Schema column names mismatch: expected {SCHEMA_V1_COLUMNS}, got {schema.names}"
        )

    for col in SCHEMA_V1_COLUMNS:
        expected_field = LAKE_SCHEMA_V1.field(col)
        actual_field = schema.field(col)

        is_type_match = (actual_field.type == expected_field.type) or (
            col == "symbol" and pa.types.is_dictionary(actual_field.type) and actual_field.type.value_type == pa.string()
        )
        if not is_type_match:
            raise SchemaValidationError(
                f"Field '{col}' type mismatch: expected {expected_field.type}, got {actual_field.type}"
            )
        if actual_field.nullable != expected_field.nullable:
            raise SchemaValidationError(
                f"Field '{col}' nullable mismatch: expected {expected_field.nullable}, got {actual_field.nullable}"
            )

    return True


def validate_table_v1(table: pa.Table) -> bool:
    """Perform vectorized validation of Lake Schema v1 constraints on a PyArrow Table."""
    if not isinstance(table, pa.Table):
        raise SchemaValidationError(f"Expected PyArrow Table, got {type(table)}")

    validate_schema_v1(table)

    # 1. Nullability check on required columns
    required_cols = ["timestamp", "symbol", "price", "ingest_id"]
    for col in required_cols:
        if table[col].null_count > 0:
            raise SchemaValidationError(f"Non-nullable column '{col}' contains {table[col].null_count} nulls")

    if table.num_rows == 0:
        return True

    # 2. Vectorized price constraints: finite and strictly positive (> 0.0)
    price_arr = table["price"]
    finite_price = pc.is_finite(price_arr)
    if not pc.all(finite_price).as_py():
        raise SchemaValidationError("Price contains non-finite values (NaN, +Inf, -Inf)")

    positive_price = pc.greater(price_arr, 0.0)
    if not pc.all(positive_price).as_py():
        raise SchemaValidationError("Price contains non-positive values (<= 0.0)")

    # 3. Vectorized nullable numeric fields: volume, bid, ask (finite and >= 0.0 if not null)
    for col in ["volume", "bid", "ask"]:
        col_arr = table[col]
        non_nulls = pc.drop_null(col_arr)
        if len(non_nulls) > 0:
            if not pc.all(pc.is_finite(non_nulls)).as_py():
                raise SchemaValidationError(f"Column '{col}' contains non-finite values (NaN, Inf)")
            if not pc.all(pc.greater_equal(non_nulls, 0.0)).as_py():
                raise SchemaValidationError(f"Column '{col}' contains negative values (< 0.0)")

    # 4. String length checks on symbol and ingest_id (> 0)
    sym_col = table["symbol"]
    if pa.types.is_dictionary(sym_col.type):
        sym_col = pc.cast(sym_col, pa.string())
    lens_sym = pc.utf8_length(sym_col)
    if not pc.all(pc.greater(lens_sym, 0)).as_py():
        raise SchemaValidationError("Column 'symbol' contains empty strings")

    iid_col = table["ingest_id"]
    lens_iid = pc.utf8_length(iid_col)
    if not pc.all(pc.greater(lens_iid, 0)).as_py():
        raise SchemaValidationError("Column 'ingest_id' contains empty strings")

    return True


def ticks_to_table(
    records: Iterable[Union[Any, Dict[str, Any], Tuple]],
    validate: bool = True,
) -> pa.Table:
    """Convert QuoteTick objects, dicts, or tuples into a validated PyArrow Table."""
    timestamps: List[Optional[datetime]] = []
    symbols: List[Optional[str]] = []
    prices: List[Optional[float]] = []
    volumes: List[Optional[float]] = []
    bids: List[Optional[float]] = []
    asks: List[Optional[float]] = []
    sources: List[Optional[str]] = []
    sessions: List[Optional[str]] = []
    ingest_ids: List[Optional[str]] = []

    for rec in records:
        if isinstance(rec, dict):
            ts = rec.get("timestamp")
            sym = rec.get("symbol")
            pr = rec.get("price")
            vol = rec.get("volume")
            bd = rec.get("bid")
            ak = rec.get("ask")
            src = rec.get("source")
            sess = rec.get("session")
            iid = rec.get("ingest_id")
        elif isinstance(rec, tuple):
            ts = rec[0] if len(rec) > 0 else None
            sym = rec[1] if len(rec) > 1 else None
            pr = rec[2] if len(rec) > 2 else None
            vol = rec[3] if len(rec) > 3 else None
            bd = rec[4] if len(rec) > 4 else None
            ak = rec[5] if len(rec) > 5 else None
            src = rec[6] if len(rec) > 6 else None
            sess = rec[7] if len(rec) > 7 else None
            iid = rec[8] if len(rec) > 8 else None
        else:
            ts = getattr(rec, "timestamp", None)
            sym = getattr(rec, "symbol", None)
            pr = getattr(rec, "price", None)
            vol = getattr(rec, "volume", None)
            bd = getattr(rec, "bid", None)
            ak = getattr(rec, "ask", None)
            src = getattr(rec, "source", None)
            sess = getattr(rec, "session", None)
            iid = getattr(rec, "ingest_id", None)

        if ts is not None:
            if isinstance(ts, str):
                ts = datetime.fromisoformat(ts)
            if hasattr(ts, "tzinfo") and ts.tzinfo is not None:
                ts = ts.astimezone(timezone.utc).replace(tzinfo=None)

        timestamps.append(ts)
        symbols.append(str(sym) if sym is not None else None)
        prices.append(float(pr) if pr is not None else None)
        volumes.append(float(vol) if vol is not None else None)
        bids.append(float(bd) if bd is not None else None)
        asks.append(float(ak) if ak is not None else None)
        sources.append(str(src) if src is not None else None)
        sessions.append(str(sess) if sess is not None else None)
        ingest_ids.append(str(iid) if iid is not None else None)

    arrays = [
        pa.array(timestamps, type=pa.timestamp("us")),
        pa.array(symbols, type=pa.string()),
        pa.array(prices, type=pa.float64()),
        pa.array(volumes, type=pa.float64()),
        pa.array(bids, type=pa.float64()),
        pa.array(asks, type=pa.float64()),
        pa.array(sources, type=pa.string()),
        pa.array(sessions, type=pa.string()),
        pa.array(ingest_ids, type=pa.string()),
    ]

    table = pa.Table.from_arrays(arrays, schema=LAKE_SCHEMA_V1)

    if validate:
        validate_table_v1(table)

    return table


def table_to_ticks(table: pa.Table) -> List[QuoteTick]:
    """Convert a PyArrow Table into a list of QuoteTick objects."""
    col_dict = table.to_pydict()
    num_rows = table.num_rows
    ticks: List[QuoteTick] = []

    for i in range(num_rows):
        ticks.append(
            QuoteTick(
                timestamp=col_dict["timestamp"][i],
                symbol=col_dict["symbol"][i],
                price=col_dict["price"][i],
                volume=col_dict["volume"][i],
                bid=col_dict["bid"][i],
                ask=col_dict["ask"][i],
                source=col_dict["source"][i],
                session=col_dict["session"][i],
                ingest_id=col_dict["ingest_id"][i],
            )
        )

    return ticks
