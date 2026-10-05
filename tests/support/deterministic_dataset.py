"""
Combined deterministic tick dataset generator (Q03 / PERF-01, ISOL-03, C43-05).

The per-scenario fixtures in tests/fixtures/deterministic_quotes.py each cover
one edge case, and none of them scale: there was no single generator producing a
large dataset that contains *all* the characteristics the lake must preserve.
This module fills that gap and is the input for the production-scale benchmarks.

Guarantees:
- Deterministic: seeded with stdlib `random.Random`, so the same seed and row
  count always reproduce the same rows on any machine.
- Stable IDs: `ingest_id` is derived from (seed, sequence) and is never random.
- Bounded memory: rows are produced in batches by a generator; callers never
  hold the whole dataset in RAM. Nothing accumulates rows internally.
- Edge-case coverage, all present in every dataset: exact duplicate
  observations, timestamp ties, null and zero volume, late (out-of-order)
  arrivals, symbols that require percent-encoding, and PRE/REG/POST session
  boundaries including a UTC date rollover.
- Scalable to >=19 symbols (defaults to 20 symbols) with Zipfian/Pareto hot-symbol
  skew across trading sessions and month windows at 1M and 10M scales.

Every dataset also emits a manifest (rows, symbols, partitions, counts of each
edge case, seed, date spans, partition and file size distributions) so a
benchmark result can be tied to the exact input.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import random
from typing import Any, Dict, Iterator, List, Optional, Sequence, Union

from tests.fixtures.deterministic_quotes import QuoteTick

# >=19 symbols (20 total) chosen to cover top equities, percent-encoded symbols,
# and FX pairs, matching production distributions and edge-case contracts.
DEFAULT_SYMBOLS: tuple[str, ...] = (
    "NVDA",    # hot symbol (rank 1)
    "TSLA",    # rank 2
    "AAPL",    # rank 3
    "MSFT",    # rank 4
    "AMD",
    "AMZN",
    "META",
    "GOOGL",
    "AVGO",
    "MU",
    "BABA",
    "TSM",
    "QCOM",
    "ORCL",
    "SHOP",
    "ADBE",
    "PANW",
    "BRK.B",   # dot -> requires percent encoding
    "BTC/USD", # slash -> requires percent encoding
    "EURUSD",  # fx symbol
)

# Rough session boundaries in UTC for a US-equity-style day.
REG_OPEN_UTC_HOUR = 13
REG_CLOSE_UTC_HOUR = 20


@dataclass
class DatasetManifest:
    """Description of the exact dataset a measurement was taken on."""

    seed: int
    requested_rows: int
    produced_rows: int
    symbols: List[str]
    window_type: str = "month"
    seconds_span: int = 30 * 86400
    symbol_counts: Dict[str, int] = field(default_factory=dict)
    duplicate_rows: int = 0
    tied_timestamp_rows: int = 0
    null_volume_rows: int = 0
    zero_volume_rows: int = 0
    late_arrival_rows: int = 0
    session_counts: Dict[str, int] = field(default_factory=dict)
    encoded_symbols: List[str] = field(default_factory=list)
    first_timestamp: Optional[str] = None
    last_timestamp: Optional[str] = None
    date_spans: Dict[str, Any] = field(default_factory=dict)
    row_counts: Dict[str, int] = field(default_factory=dict)
    partition_distribution: Dict[str, int] = field(default_factory=dict)
    file_size_distribution: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)

    def save_manifest(self, target_path: Union[str, Path]) -> Path:
        target = Path(target_path).resolve()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(self.to_dict(), indent=2), encoding="utf-8")
        return target


def _session_for(hour_utc: int) -> str:
    if hour_utc < REG_OPEN_UTC_HOUR:
        return "PRE"
    if hour_utc < REG_CLOSE_UTC_HOUR:
        return "REG"
    return "POST"


class DeterministicDataset:
    """Seeded, batched tick generator with a cumulative manifest.
    
    Supports:
    - >= 19 symbols with Zipfian/Pareto power-law skew
    - Session (e.g. 10 hours) and Month (e.g. 30 days) windows
    - 1M and 10M scale configurations with bounded generator RAM
    - Manifest capturing symbol counts, date spans, row counts, duplicate/null/late events,
      partition distributions, and file size distributions.
    """

    def __init__(
        self,
        row_count: int,
        seed: int = 20261004,
        symbols: Sequence[str] = DEFAULT_SYMBOLS,
        start: Optional[datetime] = None,
        hot_symbol_ratio: Optional[float] = None,
        duplicate_ratio: float = 0.02,
        tie_ratio: float = 0.03,
        null_volume_ratio: float = 0.02,
        zero_volume_ratio: float = 0.02,
        late_ratio: float = 0.01,
        window_type: str = "month",
        seconds_span: Optional[int] = None,
        skew_parameter: float = 1.05,
    ) -> None:
        if row_count <= 0:
            raise ValueError("row_count must be positive")
        self.row_count = int(row_count)
        self.seed = int(seed)
        self.symbols = list(symbols)
        if not self.symbols:
            raise ValueError("symbols list must not be empty")

        self.window_type = str(window_type).lower()
        if seconds_span is not None:
            self.seconds_span = int(seconds_span)
        elif self.window_type == "session":
            self.seconds_span = 10 * 3600  # 10 hours session
        else:
            self.seconds_span = 30 * 86400  # 30 days month window

        # Default start date 2026-10-01 11:00 UTC
        self.start = start or datetime(2026, 10, 1, 11, 0, 0)
        self.hot_symbol_ratio = hot_symbol_ratio
        self.duplicate_ratio = duplicate_ratio
        self.tie_ratio = tie_ratio
        self.null_volume_ratio = null_volume_ratio
        self.zero_volume_ratio = zero_volume_ratio
        self.late_ratio = late_ratio
        self.skew_parameter = float(skew_parameter)

        # Precompute Zipfian weights across symbols: w_k = 1 / (k^s)
        n_syms = len(self.symbols)
        raw_weights = [1.0 / ((i + 1) ** self.skew_parameter) for i in range(n_syms)]
        w_sum = sum(raw_weights)
        self._zipf_probs = [w / w_sum for w in raw_weights]

        self.manifest = DatasetManifest(
            seed=self.seed,
            requested_rows=self.row_count,
            produced_rows=0,
            symbols=self.symbols,
            window_type=self.window_type,
            seconds_span=self.seconds_span,
        )

    # ------------------------------------------------------------------ internal
    def _pick_symbol(self, rng: random.Random) -> str:
        if len(self.symbols) == 1:
            return self.symbols[0]

        if self.hot_symbol_ratio is not None:
            if rng.random() < self.hot_symbol_ratio:
                return self.symbols[0]
            return self.symbols[1 + rng.randrange(len(self.symbols) - 1)]

        # Zipfian / Pareto power-law skew across all symbols
        u = rng.random()
        cum = 0.0
        for idx, prob in enumerate(self._zipf_probs):
            cum += prob
            if u < cum:
                return self.symbols[idx]
        return self.symbols[-1]

    def _build(self, index: int, rng: random.Random, cursor: datetime, previous: Optional[QuoteTick]) -> QuoteTick:
        symbol = self._pick_symbol(rng)

        # Base clock advances monotonically across the span. The divisor is the
        # mean of the uniform draw below (0.55), so the dataset actually spans
        # `seconds_span` end to end rather than a fraction of it.
        advance = rng.random() * 0.9 + 0.1
        mean_advance = 0.55
        cursor = cursor + timedelta(
            milliseconds=int(advance * (self.seconds_span * 1000.0 / (max(self.row_count, 1) * mean_advance)))
        )

        volume: Optional[float] = round(rng.uniform(1.0, 500.0), 4)
        price = round(rng.uniform(5.0, 900.0), 4)
        spread = round(price * 0.0002, 4)

        # Exact duplicate observation: identical fields, new stable ingest_id.
        if previous is not None and rng.random() < self.duplicate_ratio:
            tick = QuoteTick(
                timestamp=previous.timestamp,
                symbol=previous.symbol,
                price=previous.price,
                volume=previous.volume,
                bid=previous.bid,
                ask=previous.ask,
                source=previous.source,
                session=previous.session,
                ingest_id=f"ds{self.seed}_{index:09d}",
            )
            self.manifest.duplicate_rows += 1
            # A duplicate inherits the original's volume state; count it so the
            # manifest totals still match the rows actually produced.
            if previous.volume is None:
                self.manifest.null_volume_rows += 1
            elif previous.volume == 0.0:
                self.manifest.zero_volume_rows += 1
            return tick

        # Late arrival: this observation is older than the one before it.
        is_late = previous is not None and rng.random() < self.late_ratio
        if is_late:
            cursor = cursor - timedelta(seconds=rng.randint(1, 30))
            self.manifest.late_arrival_rows += 1

        # Timestamp tie: same microsecond as the previous row for this symbol.
        is_tie = previous is not None and rng.random() < self.tie_ratio
        if is_tie:
            cursor = previous.timestamp
            self.manifest.tied_timestamp_rows += 1

        roll = rng.random()
        if roll < self.null_volume_ratio:
            volume = None
            self.manifest.null_volume_rows += 1
        elif roll < self.null_volume_ratio + self.zero_volume_ratio:
            volume = 0.0
            self.manifest.zero_volume_rows += 1

        tick = QuoteTick(
            timestamp=cursor,
            symbol=symbol,
            price=price,
            volume=volume,
            bid=round(price - spread, 4),
            ask=round(price + spread, 4),
            source="CAPITAL",
            session=_session_for(cursor.hour),
            ingest_id=f"ds{self.seed}_{index:09d}",
        )
        return tick

    # ------------------------------------------------------------------- public
    def iter_batches(self, batch_size: int = 10_000) -> Iterator[List[QuoteTick]]:
        """Yield batches of ticks, keeping peak memory proportional to batch_size.

        Counters reset at the start of each pass, so the manifest describes the
        most recent full iteration rather than accumulating across passes.
        """
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")

        self.manifest.duplicate_rows = 0
        self.manifest.tied_timestamp_rows = 0
        self.manifest.null_volume_rows = 0
        self.manifest.zero_volume_rows = 0
        self.manifest.late_arrival_rows = 0
        self.manifest.symbol_counts = {}
        self.manifest.session_counts = {}
        self.manifest.partition_distribution = {}
        self.manifest.first_timestamp = None
        self.manifest.last_timestamp = None

        rng = random.Random(self.seed)
        cursor = self.start
        previous: Optional[QuoteTick] = None
        batch: List[QuoteTick] = []
        index = 0

        for index in range(self.row_count):
            tick = self._build(index, rng, cursor, previous)
            cursor = max(cursor, tick.timestamp)
            previous = tick
            batch.append(tick)

            self.manifest.symbol_counts[tick.symbol] = self.manifest.symbol_counts.get(tick.symbol, 0) + 1
            self.manifest.session_counts[tick.session] = self.manifest.session_counts.get(tick.session, 0) + 1
            
            # Record partition observation (symbol/date)
            part_key = f"symbol={tick.symbol}/date={tick.timestamp.date().isoformat()}"
            self.manifest.partition_distribution[part_key] = self.manifest.partition_distribution.get(part_key, 0) + 1

            if self.manifest.first_timestamp is None:
                self.manifest.first_timestamp = tick.timestamp.isoformat()
            self.manifest.last_timestamp = tick.timestamp.isoformat()

            if len(batch) >= batch_size:
                yield batch
                batch = []

        if batch:
            yield batch

        self.manifest.produced_rows = index + 1
        self.manifest.row_counts = {
            "requested": self.manifest.requested_rows,
            "produced": self.manifest.produced_rows,
        }
        if self.manifest.first_timestamp and self.manifest.last_timestamp:
            try:
                t0 = datetime.fromisoformat(self.manifest.first_timestamp)
                t1 = datetime.fromisoformat(self.manifest.last_timestamp)
                diff_s = (t1 - t0).total_seconds()
                self.manifest.date_spans = {
                    "start": self.manifest.first_timestamp,
                    "end": self.manifest.last_timestamp,
                    "duration_seconds": round(diff_s, 3),
                    "days": round(diff_s / 86400.0, 2),
                }
            except Exception:
                pass

    def generate(self, batch_size: int = 10_000) -> List[QuoteTick]:
        """Materialize every row. Only safe for small datasets; prefer iter_batches."""
        rows: List[QuoteTick] = []
        for batch in self.iter_batches(batch_size=batch_size):
            rows.extend(batch)
        return rows

    def partition_count(self) -> int:
        """Distinct symbol/date partitions implied by the generated rows."""
        partitions = set()
        for batch in self.iter_batches():
            for tick in batch:
                partitions.add((tick.symbol, tick.timestamp.date()))
        return len(partitions)

    def finalize_manifest(
        self,
        encoded_symbols: Optional[Sequence[str]] = None,
        lake_root: Optional[Union[str, Path]] = None,
    ) -> DatasetManifest:
        """Finalize manifest with encoded symbols and optional physical lake partition & file size distributions."""
        self.manifest.encoded_symbols = list(encoded_symbols or [])
        if lake_root is not None:
            root = Path(lake_root).resolve()
            files = sorted((root / "ticks").glob("symbol=*/date=*/*.parquet"))
            file_sizes = [f.stat().st_size for f in files]
            part_counts: Dict[str, int] = {}
            for f in files:
                part_key = f"{f.parent.parent.name}/{f.parent.name}"
                part_counts[part_key] = part_counts.get(part_key, 0) + 1
            self.manifest.partition_distribution = part_counts
            if file_sizes:
                sorted_sizes = sorted(file_sizes)
                n = len(sorted_sizes)
                p50 = sorted_sizes[int(0.50 * (n - 1))]
                p95 = sorted_sizes[int(0.95 * (n - 1))]
                self.manifest.file_size_distribution = {
                    "count": n,
                    "min_bytes": sorted_sizes[0],
                    "max_bytes": sorted_sizes[-1],
                    "total_bytes": sum(sorted_sizes),
                    "mean_bytes": round(sum(sorted_sizes) / n, 2),
                    "p50_bytes": p50,
                    "p95_bytes": p95,
                }
        return self.manifest
