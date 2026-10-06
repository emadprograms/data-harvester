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

from src.storage.barriers import trigger_persistence_barrier
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
from src.storage.schema import QuoteTick, QuoteV2, validate_table_v1

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
        self._run_id = uuid.uuid4().hex
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
            file_namespace=self._run_id,
        )

        # Recover any uncommitted intents from previous crash
        recover_pending_publications(self.root, ownership_lock=self.publisher.lock)

        self._metrics = WriterMetrics()
        self._status = "RUNNING"
        self._seq_counter = 0
        self._batch_sequence = 0
        self._prepared_batch: Optional[Tuple[int, str, List[QuoteTick]]] = None
        self._prepared_source_signature: Optional[str] = None
        self._buffer: List[QuoteTick] = []
        self._buffer_lock = threading.RLock()
        self._publish_lock = threading.RLock()
        self._external_queue_depth = 0
        self._last_flush_monotonic = time.monotonic()
        self._metrics.last_flush_monotonic = self._last_flush_monotonic
        self._last_drop_status_write_monotonic = 0.0
        self._drop_status_write_interval_seconds = 1.0

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

    def set_operational_status(self, status: str, queue_depth: Optional[int] = None) -> None:
        """Update externally managed stream health without altering batch ownership."""
        allowed = {"RUNNING", "DEGRADED", "DRAIN_FAILED", "CLOSING", "STOPPED"}
        if status not in allowed:
            raise ValueError(f"Unsupported writer status: {status}")
        with self._publish_lock:
            if self._status == "STOPPED" and status != "STOPPED":
                raise RuntimeError("Cannot change status after the writer has stopped")
            self._status = status
            if queue_depth is not None:
                self._external_queue_depth = max(0, int(queue_depth))
            self._update_status_file()

    def _validate_and_normalize_tick(self, tick: Any) -> Tuple[bool, Optional[QuoteTick]]:
        """
        Validates symbol, price, volume, bid, ask, timestamp and assigns stable ingest_id.
        Returns (True, QuoteTick) if valid, or (False, None) if malformed.
        """
        if tick is None:
            return False, None

        if isinstance(tick, QuoteV2) or (
            isinstance(tick, dict) and ("bid_price" in tick or "ask_price" in tick)
        ):
            return self._normalize_quote_v2(tick)

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
            if len(tick) == 8:
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
            ingest_id = f"{self.writer_id}_{self._run_id}_{self._seq_counter:08d}"
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

    def _normalize_quote_v2(self, tick: Any) -> Tuple[bool, Optional[QuoteV2]]:
        """Accept a new bid/ask quote. Price, volume, and size are ignored."""
        if isinstance(tick, QuoteV2):
            ts = tick.timestamp
            symbol = tick.symbol
            bid = tick.bid_price
            ask = tick.ask_price
            source = tick.source
            session = tick.session
            ingest_id = tick.ingest_id
        elif isinstance(tick, dict):
            ts = tick.get("timestamp")
            symbol = tick.get("symbol") or tick.get("epic")
            bid = tick.get("bid_price")
            ask = tick.get("ask_price")
            source = tick.get("source", "CAPITAL")
            session = tick.get("session", "REG")
            ingest_id = tick.get("ingest_id", "")
        else:
            return False, None

        if not symbol or not isinstance(symbol, str):
            return False, None
        try:
            encode_symbol(symbol)
        except Exception:
            return False, None

        def _positive(value: Any) -> Optional[float]:
            if value is None:
                return None
            try:
                number = float(value)
            except (TypeError, ValueError):
                return None
            if math.isnan(number) or math.isinf(number) or number <= 0.0:
                return None
            return number

        bid_price = _positive(bid)
        ask_price = _positive(ask)
        if bid_price is None or ask_price is None:
            return False, None

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
            ts_dt = ts.astimezone(timezone.utc).replace(tzinfo=None) if ts.tzinfo is not None else ts
        else:
            return False, None

        if not ingest_id:
            self._seq_counter += 1
            ingest_id = f"{self.writer_id}_{self._run_id}_{self._seq_counter:08d}"
        else:
            ingest_id = str(ingest_id)

        return True, QuoteV2(
            timestamp=ts_dt,
            symbol=symbol,
            bid_price=bid_price,
            ask_price=ask_price,
            source=str(source) if source else "CAPITAL",
            session=str(session) if session else "REG",
            ingest_id=ingest_id,
        )

    def _update_status_file(self) -> None:
        """Atomically update _control/writer_status.json."""
        try:
            with self._buffer_lock:
                queue_depth = len(self._buffer) + self._external_queue_depth
            payload = {
                "status": self._status,
                "writer_id": self.writer_id,
                "pid": os.getpid(),
                "total_rows_written": self.metrics.total_rows_written,
                "batches_published": self.metrics.batches_published,
                "total_quarantined": self.metrics.total_quarantined,
                "total_retrying": self.metrics.total_retrying,
                "total_dropped": self._metrics.total_dropped,
                "queue_depth": queue_depth,
                "last_error": self._metrics.last_error,
                "last_error_time": self._metrics.last_error_time,
                "heartbeat_monotonic": time.monotonic(),
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

        trigger_persistence_barrier("admission", tick=tick)
        valid, normalized_tick = self._validate_and_normalize_tick(tick)
        if not valid:
            self._metrics.total_quarantined += 1
            return

        self._metrics.total_received += 1

        with self._buffer_lock:
            need_overflow_flush = len(self._buffer) >= self.max_queue_size

        if need_overflow_flush:
            try:
                self.flush(block=False)
            except Exception as e:
                logger.warning(f"Error during overflow flush: {e}")

        dropped = False
        should_flush = False
        with self._buffer_lock:
            if len(self._buffer) >= self.max_queue_size:
                self._metrics.total_dropped += 1
                dropped = True
            else:
                self._metrics.total_accepted += 1
                self._buffer.append(normalized_tick)
                should_flush = len(self._buffer) >= self.max_batch_rows

        if dropped:
            now_m = time.monotonic()
            if (now_m - self._last_drop_status_write_monotonic) >= self._drop_status_write_interval_seconds:
                self._last_drop_status_write_monotonic = now_m
                self._update_status_file()
            return

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
        """Publish with one stable run/batch identity across every retry."""
        with self._publish_lock:
            if not valid_ticks:
                return PublishReceipt(
                    batch_id=f"batch_{self._run_id}_{self._batch_sequence:06d}",
                    writer_id=self.writer_id,
                    sequence=self._batch_sequence,
                    row_count=0,
                    status="PUBLISHED",
                    published_at=datetime.now(timezone.utc).isoformat(),
                )

            if self._prepared_batch is None:
                self._batch_sequence += 1
                sequence = self._batch_sequence
                batch_id = f"batch_{self._run_id}_{sequence:06d}"
                self._prepared_batch = (sequence, batch_id, list(valid_ticks))
            else:
                sequence, batch_id, prepared_ticks = self._prepared_batch
                if prepared_ticks != list(valid_ticks):
                    raise PublishError(
                        f"Prepared batch {batch_id} must be retried unchanged before publishing another batch"
                    )

            last_exc = None
            for attempt in range(1, self.retry_attempts + 1):
                try:
                    receipt = self.publisher.publish_batch(
                        records_or_table=valid_ticks,
                        batch_id=batch_id,
                        sequence=sequence,
                    )
                    trigger_persistence_barrier("acknowledgment", receipt=receipt, attempt=attempt)
                    self._prepared_batch = None
                    self._prepared_source_signature = None
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
            raise PublishError(f"Batch {self._prepared_batch[1]} exhausted retries: {last_exc}")

    def publish_batch(self, ticks: Iterable[Any]) -> PublishReceipt:
        """Synchronously validates, normalizes, and publishes a batch of ticks."""
        with self._publish_lock:
            if self._status not in ("RUNNING", "CLOSING"):
                raise RuntimeError(f"Cannot publish batch when writer status is {self._status}")

            tick_list = list(ticks)
            source_signature = repr(tick_list)
            if self._prepared_batch is not None:
                if self._prepared_source_signature != source_signature:
                    raise PublishError(
                        f"Prepared batch {self._prepared_batch[1]} must be retried unchanged before publishing another batch"
                    )
                valid_ticks = list(self._prepared_batch[2])
            else:
                valid_ticks: List[QuoteTick] = []
                for tick in tick_list:
                    valid, norm_tick = self._validate_and_normalize_tick(tick)
                    if valid:
                        self._metrics.total_received += 1
                        self._metrics.total_accepted += 1
                        valid_ticks.append(norm_tick)
                    else:
                        self._metrics.total_quarantined += 1

            try:
                return self._publish_batch_core(valid_ticks)
            except Exception:
                if self._prepared_batch is not None and self._prepared_source_signature is None:
                    self._prepared_source_signature = source_signature
                raise

    async def publish_batch_async(self, ticks: Iterable[Any]) -> PublishReceipt:
        """Asynchronously publishes a batch off the event loop on worker thread."""
        loop = asyncio.get_running_loop()
        tick_list = list(ticks)
        return await loop.run_in_executor(self._executor, self.publish_batch, tick_list)

    def flush(self, block: bool = True) -> Optional[PublishReceipt]:
        """Flushes the current internal buffer."""
        acquired = self._publish_lock.acquire(blocking=block)
        if not acquired:
            return None
        try:
            with self._buffer_lock:
                if not self._buffer:
                    self._update_status_file()
                    return None
                if self._prepared_batch is not None:
                    prepared_ticks = self._prepared_batch[2]
                    if self._buffer[:len(prepared_ticks)] != prepared_ticks:
                        raise PublishError(
                            f"Prepared batch {self._prepared_batch[1]} is not at the head of the writer buffer"
                        )
                    batch = list(prepared_ticks)
                    del self._buffer[:len(prepared_ticks)]
                else:
                    batch = list(self._buffer)
                    self._buffer.clear()
                self._last_flush_monotonic = time.monotonic()
                self._metrics.last_flush_monotonic = self._last_flush_monotonic

            try:
                return self._publish_batch_core(batch)
            except Exception:
                with self._buffer_lock:
                    self._buffer = batch + self._buffer
                raise
        finally:
            self._publish_lock.release()

    async def flush_async(self) -> Optional[PublishReceipt]:
        """Asynchronously flushes the current internal buffer on the worker thread."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, self.flush)

    # Compatibility method aliases
    enqueue_tick = write_tick
    enqueue_ticks = write_ticks
    flush_batch = publish_batch
    flush_all = flush

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
