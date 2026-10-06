# 🚀 Stock Data Harvester & Observability Engine (v5.0)

A high-performance market data harvesting, 24/7 live tick streaming, and telemetry observability engine powered by a **Partitioned Parquet Tick Lake**, zero-lock multi-process concurrency, sub-100ms in-memory **DuckDB** analytical resampling, and interactive financial charting.

**Current milestone:** v6.0 checkout is complete. New rows store bid and ask prices. The lake already on disk is still schema v1 until the owner runs the rewrite locally. v5.0 remains the storage baseline: the partitioned Parquet tick lake is the **only** store, no runtime path creates a `.duckdb` file, and the streaming scope is the 19 approved US equities, weekdays 04:00–20:00 ET.

New rows store `bid_price` and `ask_price` only, plus `timestamp`, `symbol`, `source`, `session`, and `ingest_id`. Do not read `price` or `volume` as the stored quote. Capital.com stores its quote-change bid and ask. Databento `tbbo` stores `bid_px_00` as `bid_price` and `ask_px_00` as `ask_price`. Fewer Databento rows than Capital.com rows is accepted.

The existing lake is still schema v1. Those files still contain `price`, `volume`, `bid`, and `ask`. The quote in a schema v1 file is `bid` and `ask`, not `price` or `volume`. The rewrite that copies `bid` to `bid_price` and `ask` to `ask_price` is available as `python -m src.storage.quote_rewrite --lake-root <explicit-lake> --backup-root <explicit-backup>`. Production execution is deferred to the owner. Do not run that rewrite against the production lake from this checkout.

Gap fill names one day and requests Databento `tbbo` only for stretches inside 04:00-20:00 ET where every active registry symbol is silent. A minute where one symbol has a tick is not requested. Pre-market (04:00-09:30 ET) and post-market (16:00-20:00 ET) count at 15 minutes or more. Regular hours (09:30-16:00 ET) count at 2 minutes or more. A stretch that crosses 09:30 or 16:00 is split, and each piece uses its own rule. Weekends, full NYSE holidays, and the time after an official early close (13:00 ET) are not requested. Gap fill does not start while the live writer holds the publisher lock. It appends new rows and does not rewrite existing files. Name exactly one day with `python -m src.data.gap_fill --date YYYY-MM-DD`.

A new-row candle uses `bid_price`. This query does not match files already on disk:

```sql
SELECT
    time_bucket(INTERVAL '1 minute', timestamp) AS bucket_time,
    arg_min(bid_price, (timestamp, ingest_id)) AS open,
    max(bid_price) AS high,
    min(bid_price) AS low,
    arg_max(bid_price, (timestamp, ingest_id)) AS close
FROM read_parquet(?)
GROUP BY bucket_time
```

---

## 🏛 Architecture Overview

### 🌊 Partitioned Parquet Tick Lake (`TICK_LAKE_ROOT`, default `data/tick_lake`)
- **Immutable Hive Partitioning**: Live market ticks are streamed 24/7 and committed into immutable Parquet files organized by Hive two-level partitioning:
  ```text
  <TICK_LAKE_ROOT>/ticks/symbol=<SYMBOL>/date=<YYYY-MM-DD>/batch_<WRITER_ID>_<SEQ>.parquet
  ```
- **Zero-Lock Concurrency**: Downstream consumers (e.g. Repo B quantitative strategies, analytical runners, dashboard processes) query live market data using isolated in-memory DuckDB connections (`:memory:`). Ingestion writers and concurrent reader processes operate simultaneously with **zero file-lock collisions** (`duckdb.IOException`), zero read latency degradation, and zero Parquet footer corruption.
- **Sub-100ms Resampling**: In-memory DuckDB vectorized queries resample ticks into bars (`1s`, `5s`, `1m`, `5m`, `15m`, `1h`, `1d`). New rows use `bid_price`. Files already on disk are still schema v1, and `price` and `volume` are not the stored quote.
- **Atomic Two-Phase Publication**: Writers buffer ticks in memory, write staged Parquet files with checksum verification, and atomically promote them via filesystem rename into `ticks/` with fsynced publication receipts in `_control/receipts/`.
- **Versioned Symbol Registry**: Symbol lifecycle (activation, deactivation, purge fencing) is centrally managed in `_control/registry.json` with monotonic version bumping and cross-process reload signaling (`.stream_reload.signal`).
- **Control Plane Layout**: `lake.json` (format metadata), `_staging/` (in-flight writes), `_maintenance/` (guard + compaction artifacts), `_migration/` (migration plan/state/verification), `_control/` (registry, writer status, publisher lock, receipts, intents), `_spool/` (optional durable spool).

### 💾 One Store, One Mental Model
- Bars are gone: the 1-minute historical archive and its harvesters were deleted in v5.0, so there is a single data model — **raw ticks in the lake**, resampled on demand.
- Candles are computed from ticks by the in-memory query engine; nothing is pre-aggregated on disk.
- The symbol registry (`_control/registry.json`) is the only symbol authority (19 equities); removing a symbol fences it immediately and off-hours compaction drops its partitions.

### ⏱ UTC Storage Mandate & NYSE Exchange-Time Rendering
- Every stored timestamp in the lake is pure UTC with microsecond precision.
- Every in-memory DuckDB session explicitly sets `SET TimeZone = 'UTC'` to prevent host OS timezone contamination.
- **NYSE Exchange-Time Rendering**: The web UI, chart axes, crosshair badges, and CSV exports render exchange-local timestamps in `America/New_York` (EST/EDT), guaranteeing the 09:30 AM ET opening bell always renders at 09:30 ET regardless of browser or server location.

### 🌐 Observability Command Center Dashboard (`http://localhost:8420`)
- Multi-threaded local HTTP server providing interactive TradingView Lightweight Charts (v4.1.3).
- Real-time tick stream tape with bid, ask, and spread tracking.
- Automated integrity auditing: stream quiet-interval checks, OHLCV geometric sanity validation, and the continuity / gap analysis that shades missing data on the chart.
- Symbol management (add / fence) with streamer hot-reload, a live stream tape, and continuity ribbons for every monitored equity.

### 🛡️ Hardening & Verification (v4.1)
- 6 phase-specific stress suites covering storage/publication edges, writer/runner lifecycles, registry concurrency, in-memory reader scaling, migration fuzzing, and multi-process soak/chaos.
- 30+ concurrent in-memory DuckDB readers with no memory leaks across 1,000+ sequential queries.
- Crash-intent recovery, malformed-tick quarantine, disk-full backoff, and supervisor self-healing under chaos-monkey termination are all covered by executable tests.
- **Audit status:** the independent v4.1 audit ([report](.planning/v4.1-MILESTONE-AUDIT.md)) confirms 688/688 passing and **no data-corrupting defects**, but records `gaps_found` because no per-phase `VERIFICATION.md` artifacts were produced (process gap) and because two tests only partially exercise the production paths (real SIGTERM handling; chaos against the real streamer). Remaining items are tracked in the [roadmap backlog](.planning/ROADMAP.md).

---

## 🔌 Downstream Integration: Repo B Reader Contract

Downstream consumers (such as Repo B) require **zero imports** from `data-harvester`. You only need standard, publicly available `duckdb >= 1.0.0` or `pyarrow >= 14.0.0`. The full contract (schema table, partition pruning rules, maintenance-guard rules, and query snippets) lives in [`docs/contracts/repo_b_tick_lake_contract.md`](docs/contracts/repo_b_tick_lake_contract.md).

### Standalone Python Reader Snippet

The snippet below reads files already on disk. Those files are still schema v1. `price` and `volume` in that snippet are not the stored quote. New rows use `bid_price` and `ask_price`.

```python
from datetime import date, datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
import duckdb


class RepoBTickReader:
    """
    Zero-dependency reader for Partitioned Parquet Tick Lake.
    Can be copy-pasted directly into Repo B or downstream pipelines.
    Requires only standard `duckdb`.
    """
    def __init__(self, lake_root: str):
        self.lake_root = Path(lake_root).resolve()
        self.ticks_dir = self.lake_root / "ticks"

    def _resolve_files(self, symbol: str, start_date: date, end_date: date) -> List[str]:
        """Prune partitions at filesystem level before passing to DuckDB."""
        sym_dir = self.ticks_dir / f"symbol={symbol.upper()}"
        if not sym_dir.is_dir():
            return []

        matched_files: List[str] = []
        for date_dir in sym_dir.iterdir():
            if not date_dir.is_dir() or not date_dir.name.startswith("date="):
                continue
            try:
                dir_date = date.fromisoformat(date_dir.name.split("=")[1])
                if start_date <= dir_date <= end_date:
                    for parquet_file in date_dir.glob("*.parquet"):
                        matched_files.append(str(parquet_file))
            except ValueError:
                continue

        return sorted(matched_files)

    def query_candles(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        timeframe: str = "1m",
    ) -> List[Dict[str, Any]]:
        """
        Resample raw ticks. This example reads files already on disk.
        Those files are still schema v1. price and volume here are not the stored quote.
        """
        files = self._resolve_files(symbol, start_date, end_date)
        if not files:
            return []

        interval_map = {
            "1s": "1 second",
            "5s": "5 seconds",
            "1m": "1 minute",
            "5m": "5 minutes",
            "15m": "15 minutes",
            "1h": "1 hour",
            "1d": "1 day",
        }
        interval_str = interval_map.get(timeframe.lower(), "1 minute")

        # Isolated in-memory connection — ZERO lock contention with live writers
        con = duckdb.connect(":memory:")
        try:
            con.execute("SET TimeZone = 'UTC'")
            query = f"""
                SELECT
                    time_bucket(INTERVAL '{interval_str}', timestamp) AS bucket_time,
                    symbol,
                    arg_min(price, (timestamp, ingest_id)) AS open,
                    max(price) AS high,
                    min(price) AS low,
                    arg_max(price, (timestamp, ingest_id)) AS close,
                    sum(coalesce(volume, 1.0)) AS volume,
                    count(*) AS tick_count
                FROM read_parquet(?, hive_partitioning=false)
                GROUP BY bucket_time, symbol
                ORDER BY bucket_time ASC, symbol ASC
            """
            rows = con.execute(query, [files]).fetchall()
            return [
                {
                    "time": row[0],
                    "symbol": row[1],
                    "open": float(row[2]),
                    "high": float(row[3]),
                    "low": float(row[4]),
                    "close": float(row[5]),
                    "volume": float(row[6]),
                    "tick_count": int(row[7]),
                }
                for row in rows
            ]
        finally:
            con.close()
```

---

## ⚙️ Configuration & Environment Variables

Configure lake paths and credentials in your `.env` file (see `.env.example`):

```bash
# Tick Lake Storage Path (default: <DATA_DIR>/tick_lake)
TICK_LAKE_ROOT=data/tick_lake

# Base directory for historical database and default tick_lake parent
DATA_DIR=data

# Dashboard HTTP port (fallback ports 8421/8422/8425 are tried if 8420 is busy)
DASHBOARD_PORT=8420

# Capital.com API Credentials
CAPITAL_COM_X_CAP_API_KEY=your_key
CAPITAL_COM_IDENTIFIER=your_identifier
CAPITAL_COM_PASSWORD=your_password
```

Lake root resolution precedence (`src/storage/config.py`): explicit argument → `TICK_LAKE_ROOT` → `DATA_DIR/tick_lake` → `/Volumes/Micron-E 0256 A/data-harvester/data/tick_lake` (if mounted) → `<repo_root>/data/tick_lake` → error.

### Ingestion & Writer Tuning

The runtime knobs are constructor arguments, not (yet) environment variables:

| Knob | Where | Default |
|---|---|---|
| Flush interval | `StreamingEngine(flush_interval=...)` → `TickLakeWriter.flush_interval_seconds` | `2.0s` in the runner; `5.0s` writer-class default |
| Max batch rows | `TickLakeWriter(max_batch_rows=...)` | `5000` |
| Queue capacity | `StreamingEngine(max_queue_size=...)` / `TickLakeWriter(max_queue_size=...)` | `10000` |
| Compression | `TickLakeWriter(compression=...)` | `snappy` |
| Reader threads / memory | `TickLakeReader(max_threads=..., max_memory=...)` | `4` threads / `2GB` |
| Registry poll / debounce | `StreamingEngine(registry_poll_interval=..., registry_debounce_interval=...)` | `1.0s` / `0.05s` |
| Writer retries | `TickLakeWriter(retry_attempts=..., retry_backoff_base=...)` | `3` attempts, `0.01s` base |

> ⚠️ `STREAM_FLUSH_INTERVAL`, `STREAM_MAX_BATCH_ROWS`, `STREAM_MAX_QUEUE_SIZE`, and `STREAM_COMPRESSION` are documented in `.env.example` but are **not yet read by the runtime** (tracked as a backlog item in `.planning/ROADMAP.md`). Configure the writer/runner through the constructor arguments above until that wiring lands.

---

## 🚀 Operational Commands & Validation

### 🍎 macOS 24/7 Always-On Services
- **Start All Services**: `./START_SERVICES.sh` (or `./tools/mac/start_services.sh`)
- **Check Status & Storage**: `./VIEW_STATUS.sh` (or `./tools/mac/status_services.sh`)
- **Stop All Services**: `./STOP_SERVICES.sh` (or `./tools/mac/stop_services.sh`)
- **Start Streamer Only**: `./tools/mac/start_streamer.sh`
- **Start Dashboard Only**: `./tools/mac/start_dashboard.sh`
- **Launchd Auto-Start**: `./tools/mac/install_startup.sh` / `./tools/mac/uninstall_startup.sh`

### ⚡ Concurrency & Multi-Process Validation
Validate high-concurrency invariants (zero reader I/O lock errors, zero Parquet footer corruption, writer event-loop lag < 20ms, dashboard & Repo B p95 latency < 100ms):
```bash
python tools/validate_concurrency.py
```
Options: `--ticks` (default 6000), `--lake-root`, `--dashboard-requests` (default 160), `--repo-b-iterations` (default 60), `--output-json`, `--host`, `--port`.

### 🔍 Lake Audit & Environment Preflight
```bash
# Audit lake integrity (partition inventory, schema, orphan/gap checks)
python3 tools/migrate_streaming_to_parquet.py audit-lake --lake-root data/tick_lake

# Verify the local environment before starting the streamer
python3 tools/preflight.py
```

The one-time legacy migration, the deletion of the two `.duckdb` files and the milestone
close-out are owner actions, step by step in
[`docs/operations/phase49_migration_runbook.md`](docs/operations/phase49_migration_runbook.md).
One command runs the whole migration gate (and never deletes anything):
`./tools/mac/run_phase49_migration.sh`.

### 🧗 Hardening & Chaos Suites
```bash
pytest tests/storage/test_storage_edge_cases.py -v    # path traversal, schema edges, publication collisions (36)
pytest tests/stream/test_lake_runner_stress.py -v     # micro-batch stress, shutdown drain, disk-full backoff (14)
pytest tests/storage/test_registry_stress.py -v       # cross-process CRUD, PENDING_PURGE, signal storms (13)
pytest tests/storage/test_lake_reader_stress.py -v    # 30+ concurrent readers, DST/leap-year edges (17)
pytest tests/storage/test_migration_stress.py -v      # crash interruption + EXCEPT ALL fuzzing (32)
pytest tests/integration/ -v                          # multi-process soak + chaos monkey self-healing (10 + concurrency)
```

### 🧪 Running Tests
Run the entire offline test suite (live and performance suites are marked and can be excluded):
```bash
pytest tests/ -m "not live and not performance" -v
```
Or targeted subsystems:
```bash
pytest tests/storage/ -v       # Lake config/schema/publication/registry/reader/writer + hardening tests
pytest tests/stream/ -v        # 24/7 WebSocket streamer, backpressure, and runner lifecycle tests
pytest tests/dashboard/ -v     # Dashboard server & analytics tests
pytest tests/integration/ -v   # Multi-process concurrency, soak, and chaos tests
```

> ℹ️ Timing-sensitive gates (supervisor soak p95 latency, signal-storm debounce) depend on host speed. Run them on the named reference machine described in [`docs/plans/tick-lake-test-first-remediation.md`](docs/plans/tick-lake-test-first-remediation.md) before treating a threshold miss as a regression.

---

## 📚 Documentation Index

| Document | Contents |
|---|---|
| [docs/operations/tick_lake_operations_guide.md](docs/operations/tick_lake_operations_guide.md) | Production operations: architecture, configuration, service management, compaction, migration, disaster recovery |
| [docs/contracts/repo_b_tick_lake_contract.md](docs/contracts/repo_b_tick_lake_contract.md) | Read-only integration contract for downstream consumers (Repo B) |
| [docs/plans/partitioned-parquet-tick-lake.md](docs/plans/partitioned-parquet-tick-lake.md) | Historical design plan that produced the v4.0 lake (implemented) |
| [docs/plans/tick-lake-test-first-remediation.md](docs/plans/tick-lake-test-first-remediation.md) | Historical test-first remediation plan (executed across v4.0/v4.1) |
| [.planning/v4.1-MILESTONE-AUDIT.md](.planning/v4.1-MILESTONE-AUDIT.md) | Independent v4.1 audit: verdict, requirement cross-reference, integration findings, tech debt |
| [.planning/MILESTONES.md](.planning/MILESTONES.md) | Shipped milestone history (v1.0 → v4.1) |
| [.planning/ROADMAP.md](.planning/ROADMAP.md) | Milestone roadmap and unplanned backlog (incl. audit follow-ups) |
| [.planning/PROJECT.md](.planning/PROJECT.md) | Project overview, validated requirements, and key decisions |

---

## 📜 Milestones & Roadmap
Full details on shipped milestones (v1.0 → v4.3) are tracked in [.planning/MILESTONES.md](.planning/MILESTONES.md). v5.0 is the active milestone: Parquet-only storage, a weekday 04:00–20:00 ET ingestion window, off-hours compaction, and macOS launchd scheduling. Progress lives in [.planning/STATE.md](.planning/STATE.md) and [.planning/REQUIREMENTS.md](.planning/REQUIREMENTS.md).
