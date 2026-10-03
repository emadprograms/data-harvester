"""
Micro-batch Parquet writer and metrics for Partitioned Parquet Tick Lake (Milestone v4.0 - Phase 17).
"""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
import math
import os
from pathlib import Path
import threading
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
import uuid

import pyarrow as pa

from src.storage.config import (
    encode_symbol,
    init_tick_lake,
    resolve_tick_lake_root,
)
from src.storage.publication import (
    LakePublisher,
    LakePublisherLock,
    PublishError,
    PublishReceipt,
    recover_pending_publications,
)
from src.storage.schema import QuoteTick, validate_table_v1

logger = logging.getLogger("parquet_writer")


@dataclass
class WriterMetrics:
    """Metrics tracking write throughput, batch latency, retries, and quarantines."""
    total_received: int = 0
    total_accepted: int = 0
    total_published: int = 0
    total_retrying: int = 0
    total_quarantined: int = 0
    total_dropped: int = 0
    batches_published: int = 0
    last_flush_monotonic: float = 0.0
    last_publish_time: str = ""
    last_batch_id: str = ""
    last_batch_rows: int = 0
    last_error: Optional[str] = None
    last_error_time: Optional[str] = None

    @property
    def total_rows_written(self) -> int:
        return self.total_published


class TickLakeWriter:
    """
    Micro-batch Parquet writer for the Partitioned Parquet Tick Lake.

    Provides bounded buffer management, off-loop PyArrow writes, automatic retries
    with backoff, malformed tick quarantining, and single-writer ownership enforcement.
    """

    def __init__(
        self,
        root: Union[str, Path],
        writer_id: str = "writer_1",
        max_batch_rows: int = 5000,
        flush_interval_seconds: float = 5.0,
        max_queue_size: int = 10000,
        compression: str = "snappy",
        auto_init: bool = True,
        retry_attempts: int = 3,
        retry_backoff_base: float = 0.01,
    ):
        self.root = resolve_tick_lake_root(root)
        self.writer_id = writer_id
        self.max_batch_rows = max_batch_rows
        self.flush_interval_seconds = flush_interval_seconds
        self.max_queue_size = max_queue_size
        self.compression = compression
        self.auto_init = auto_init
        self.retry_attempts = retry_attempts
        self.retry_backoff_base = retry_backoff_base

        if self.auto_init:
            init_tick_lake(self.root)

        # Acquire single-writer lock via LakePublisher
        self.publisher = LakePublisher(
            root=self.root,
            writer_id=self.writer_id,
            compression=self.compression,
        )

        # Recover any uncommitted intents from previous crash
        recover_pending_publications(self.root)

        self._metrics = WriterMetrics()
        self._status = "RUNNING"
        self._seq_counter = 0
        self._batch_sequence = 0
        self._buffer: List[QuoteTick] = []
        self._buffer_lock = threading.Lock()
        self._publish_lock = threading.RLock()
        self._last_flush_monotonic = time.monotonic()
        self._metrics.last_flush_monotonic = self._last_flush_monotonic

        self._control_dir = self.root / "_control"
        self._staging_dir = self.root / "_staging"
        self._status_file = self._control_dir / "writer_status.json"

        # Write initial RUNNING status file
        self._update_status_file()

        # Dedicated single-worker thread pool for off-loop PyArrow writes
        self._executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix=f"lake_writer_{self.writer_id}",
        )

        # Background monotonic clock flusher thread
        self._stop_flusher = threading.Event()
        self._flusher_thread = threading.Thread(
            target=self._flusher_worker,
            name=f"tick_flusher_{self.writer_id}",
            daemon=True,
        )
        self._flusher_thread.start()

    @property
    def metrics(self) -> WriterMetrics:
        return self._metrics

    @property
    def status(self) -> str:
        return self._status

    def _validate_and_normalize_tick(self, tick: Any) -> Tuple[bool, Optional[QuoteTick]]:
        """
        Validates symbol, price, volume, bid, ask, timestamp and assigns stable ingest_id.
        Returns (True, QuoteTick) if valid, or (False, None) if malformed.
        """
        if tick is None:
            return False, None

        if isinstance(tick, QuoteTick):
            ts = tick.timestamp
            symbol = tick.symbol
            price = tick.price
            volume = tick.volume
            bid = tick.bid
            ask = tick.ask
            source = tick.source
            session = tick.session
            ingest_id = tick.ingest_id
        elif isinstance(tick, dict):
            ts = tick.get("timestamp")
            symbol = tick.get("symbol") or tick.get("epic")
            price = tick.get("price")
            volume = tick.get("volume")
            bid = tick.get("bid")
            ask = tick.get("ask")
            source = tick.get("source", "CAPITAL")
            session = tick.get("session", "REG")
            ingest_id = tick.get("ingest_id", "")
        elif isinstance(tick, tuple):
            if len(tick) == 9 and tick[8] in ("CAPITAL", "BINANCE", "SIMULATED", "MANUAL"):
                # Bar tuple: (ts, sym, open, high, low, close, volume, session, source)
                ts = tick[0]
                symbol = tick[1]
                price = tick[5]
                volume = tick[6]
                bid = None
                ask = None
                session = tick[7]
                source = tick[8]
                ingest_id = ""
            elif len(tick) == 8:
                # Standard tick tuple: (ts, symbol, price, volume, bid, ask, source, session)
                ts, symbol, price, volume, bid, ask, source, session = tick
                ingest_id = ""
            elif len(tick) >= 9:
                ts, symbol, price, volume, bid, ask, source, session, ingest_id = tick[:9]
            else:
                return False, None
        else:
            ts = getattr(tick, "timestamp", None)
            symbol = getattr(tick, "symbol", None)
            price = getattr(tick, "price", None)
            volume = getattr(tick, "volume", None)
            bid = getattr(tick, "bid", None)
            ask = getattr(tick, "ask", None)
            source = getattr(tick, "source", "CAPITAL")
            session = getattr(tick, "session", "REG")
            ingest_id = getattr(tick, "ingest_id", "")

        # 1. Symbol validation (non-empty, safe chars, no path traversal)
        if not symbol or not isinstance(symbol, str):
            return False, None
        try:
            encode_symbol(symbol)
        except Exception:
            return False, None

        # 2. Price validation (finite, > 0.0)
        if price is None:
            return False, None
        try:
            p = float(price)
        except (TypeError, ValueError):
            return False, None
        if math.isnan(p) or math.isinf(p) or p <= 0.0:
            return False, None

        # 3. Volume, bid, ask validation (finite, >= 0.0 if present)
        v = None
        if volume is not None:
            try:
                v = float(volume)
                if math.isnan(v) or math.isinf(v) or v < 0.0:
                    return False, None
            except (TypeError, ValueError):
                return False, None

        b = None
        if bid is not None:
            try:
                b = float(bid)
                if math.isnan(b) or math.isinf(b) or b < 0.0:
                    return False, None
            except (TypeError, ValueError):
                return False, None

        a = None
        if ask is not None:
            try:
                a = float(ask)
                if math.isnan(a) or math.isinf(a) or a < 0.0:
                    return False, None
            except (TypeError, ValueError):
                return False, None

        # 4. Timestamp normalization
        if ts is None:
            ts_dt = datetime.now(timezone.utc).replace(tzinfo=None)
        elif isinstance(ts, str):
            try:
                ts_dt = datetime.fromisoformat(ts)
                if ts_dt.tzinfo is not None:
                    ts_dt = ts_dt.astimezone(timezone.utc).replace(tzinfo=None)
            except Exception:
                return False, None
        elif isinstance(ts, datetime):
            if ts.tzinfo is not None:
                ts_dt = ts.astimezone(timezone.utc).replace(tzinfo=None)
            else:
                ts_dt = ts
        else:
            return False, None

        # 5. Ingest ID (assign deterministic/sequential if missing)
        if not ingest_id:
            self._seq_counter += 1
            ingest_id = f"{self.writer_id}_{self._seq_counter:08d}"
        else:
            ingest_id = str(ingest_id)

        return True, QuoteTick(
            timestamp=ts_dt,
            symbol=symbol,
            price=p,
            volume=v,
            bid=b,
            ask=a,
            source=str(source) if source else "CAPITAL",
            session=str(session) if session else "REG",
            ingest_id=ingest_id,
        )

    def _update_status_file(self) -> None:
        """Atomically update _control/writer_status.json."""
        try:
            payload = {
                "status": self._status,
                "writer_id": self.writer_id,
                "pid": os.getpid(),
                "total_rows_written": self.metrics.total_rows_written,
                "batches_published": self.metrics.batches_published,
                "total_quarantined": self.metrics.total_quarantined,
                "total_retrying": self.metrics.total_retrying,
                "last_publish_time": self.metrics.last_publish_time,
                "last_batch_id": self.metrics.last_batch_id,
                "last_batch_rows": self.metrics.last_batch_rows,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            self._control_dir.mkdir(parents=True, exist_ok=True)
            self._staging_dir.mkdir(parents=True, exist_ok=True)
            tmp_path = self._staging_dir / f"tmp_status_{uuid.uuid4().hex}.json"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, self._status_file)
        except Exception as e:
            logger.warning(f"Failed to update writer status file: {e}")

    def _flusher_worker(self) -> None:
        """Background thread monitoring buffer age against flush_interval_seconds."""
        while not self._stop_flusher.is_set():
            self._stop_flusher.wait(timeout=min(0.02, max(0.005, self.flush_interval_seconds / 4.0)))
            if self._stop_flusher.is_set():
                break
            with self._buffer_lock:
                has_items = len(self._buffer) > 0
                age = time.monotonic() - self._last_flush_monotonic
            if has_items and age >= self.flush_interval_seconds:
                try:
                    self.flush()
                except Exception as e:
                    logger.error(f"TickLakeWriter background flusher error: {e}")

    def write_tick(self, tick: Any) -> None:
        """Enqueue an individual tick. Triggers flush if max_batch_rows reached."""
        if self._status != "RUNNING":
            raise RuntimeError(f"Cannot write tick when writer status is {self._status}")

        valid, normalized_tick = self._validate_and_normalize_tick(tick)
        if not valid:
            self._metrics.total_quarantined += 1
            return

        self._metrics.total_received += 1
        self._metrics.total_accepted += 1

        should_flush = False
        with self._buffer_lock:
            self._buffer.append(normalized_tick)
            if len(self._buffer) >= self.max_batch_rows:
                should_flush = True

        if should_flush:
            self.flush()

    def write_ticks(self, ticks: Iterable[Any]) -> None:
        """Enqueue multiple ticks."""
        for tick in ticks:
            self.write_tick(tick)

    async def write_tick_async(self, tick: Any) -> None:
        """Async enqueue an individual tick."""
        self.write_tick(tick)

    def _publish_batch_core(self, valid_ticks: List[QuoteTick]) -> PublishReceipt:
        """Core batch publication with retry backoff and metrics tracking."""
        with self._publish_lock:
            if not valid_ticks:
                return PublishReceipt(
                    batch_id=f"batch_{self.writer_id}_{self._batch_sequence:06d}",
                    writer_id=self.writer_id,
                    sequence=self._batch_sequence,
                    row_count=0,
                    status="PUBLISHED",
                    published_at=datetime.now(timezone.utc).isoformat(),
                )

            self._batch_sequence += 1
            seq = self._batch_sequence
            batch_id = f"batch_{self.writer_id}_{seq:06d}"

            last_exc = None
            for attempt in range(1, self.retry_attempts + 1):
                try:
                    receipt = self.publisher.publish_batch(
                        records_or_table=valid_ticks,
                        batch_id=batch_id,
                        sequence=seq,
                    )
                    self._metrics.total_published += receipt.row_count
                    self._metrics.batches_published += 1
                    self._metrics.last_batch_id = receipt.batch_id
                    self._metrics.last_batch_rows = receipt.row_count
                    self._metrics.last_publish_time = receipt.published_at
                    self._update_status_file()
                    return receipt
                except (OSError, IOError, PublishError) as exc:
                    last_exc = exc
                    self._metrics.total_retrying += 1
                    self._metrics.last_error = str(exc)
                    self._metrics.last_error_time = datetime.now(timezone.utc).isoformat()
                    if attempt < self.retry_attempts:
                        backoff = self.retry_backoff_base * (2 ** (attempt - 1))
                        time.sleep(backoff)
                    else:
                        self._update_status_file()
                        raise

    def publish_batch(self, ticks: Iterable[Any]) -> PublishReceipt:
        """Synchronously validates, normalizes, and publishes a batch of ticks."""
        if self._status not in ("RUNNING", "CLOSING"):
            raise RuntimeError(f"Cannot publish batch when writer status is {self._status}")

        valid_ticks: List[QuoteTick] = []
        for t in ticks:
            valid, norm_tick = self._validate_and_normalize_tick(t)
            if valid:
                self._metrics.total_received += 1
                self._metrics.total_accepted += 1
                valid_ticks.append(norm_tick)
            else:
                self._metrics.total_quarantined += 1

        return self._publish_batch_core(valid_ticks)

    async def publish_batch_async(self, ticks: Iterable[Any]) -> PublishReceipt:
        """Asynchronously publishes a batch off the event loop on worker thread."""
        loop = asyncio.get_running_loop()
        tick_list = list(ticks)
        return await loop.run_in_executor(self._executor, self.publish_batch, tick_list)

    def flush(self) -> Optional[PublishReceipt]:
        """Flushes the current internal buffer."""
        with self._publish_lock:
            with self._buffer_lock:
                if not self._buffer:
                    return None
                batch = list(self._buffer)
                self._buffer.clear()
                self._last_flush_monotonic = time.monotonic()
                self._metrics.last_flush_monotonic = self._last_flush_monotonic

            return self._publish_batch_core(batch)

    async def flush_async(self) -> Optional[PublishReceipt]:
        """Asynchronously flushes the current internal buffer on the worker thread."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, self.flush)

    def close(self) -> None:
        """Stops background workers, flushes remaining buffer, and releases publisher lock."""
        if self._status == "STOPPED":
            return
        self._status = "CLOSING"

        # Signal flusher thread to stop
        self._stop_flusher.set()
        if self._flusher_thread.is_alive() and threading.current_thread() != self._flusher_thread:
            self._flusher_thread.join(timeout=2.0)

        # Flush any remaining buffered ticks
        try:
            self.flush()
        except Exception as e:
            logger.error(f"Error flushing during writer close: {e}")

        # Shutdown executor thread pool
        self._executor.shutdown(wait=True)

        # Mark STOPPED in status file
        self._status = "STOPPED"
        try:
            self._update_status_file()
        except Exception:
            pass

        # Release advisory publisher lock
        self.publisher.close()

    async def close_async(self) -> None:
        """Asynchronously closes the writer on an executor thread."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.close)

    def __enter__(self) -> "TickLakeWriter":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass
