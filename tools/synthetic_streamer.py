"""
Synthetic Tick Streamer Helper for Multi-Process Soak & Chaos Testing.
Can be executed as a standalone module via `python -m tools.synthetic_streamer`.
"""
import argparse
import asyncio
from datetime import datetime, timezone, timedelta
import os
from pathlib import Path
import signal
import sys
import time

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.storage.config import init_tick_lake
from src.storage.parquet_writer import TickLakeWriter

SYMBOLS = ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN"]


class SyntheticStreamer:
    def __init__(
        self,
        lake_root: Path,
        writer_id: str = "streamer_synthetic",
        batch_size: int = 50,
        flush_interval: float = 0.2,
        ticks_per_second: float = 200.0,
        drain_delay_on_term: float = 0.5,
    ):
        self.lake_root = Path(lake_root).resolve()
        init_tick_lake(self.lake_root)
        self.writer = TickLakeWriter(
            root=self.lake_root,
            writer_id=writer_id,
            max_batch_rows=batch_size,
            flush_interval_seconds=flush_interval,
        )
        self.batch_size = batch_size
        self.sleep_between_ticks = 1.0 / ticks_per_second if ticks_per_second > 0 else 0.005
        self.drain_delay_on_term = drain_delay_on_term
        self.running = True
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=10000)

    async def _producer(self):
        idx = 0
        t_base = datetime.now(timezone.utc).replace(tzinfo=None)
        while self.running:
            sym = SYMBOLS[idx % len(SYMBOLS)]
            tick = {
                "timestamp": t_base + timedelta(milliseconds=idx * 10),
                "symbol": sym,
                "price": 150.0 + (idx % 100) * 0.1,
                "volume": 10.0,
                "bid": 149.95,
                "ask": 150.05,
                "source": "SYNTHETIC",
                "session": "REG",
            }
            try:
                self.queue.put_nowait(tick)
            except asyncio.QueueFull:
                pass
            idx += 1
            await asyncio.sleep(self.sleep_between_ticks)

    async def _consumer(self):
        batch = []
        while self.running or not self.queue.empty():
            try:
                tick = await asyncio.wait_for(self.queue.get(), timeout=0.1)
                batch.append(tick)
                self.queue.task_done()
            except asyncio.TimeoutError:
                pass

            if len(batch) >= self.batch_size or (not self.running and batch):
                to_write = list(batch)
                batch.clear()
                await self.writer.publish_batch_async(to_write)

        if batch:
            await self.writer.publish_batch_async(batch)

    async def run(self):
        producer_task = asyncio.create_task(self._producer())
        consumer_task = asyncio.create_task(self._consumer())
        await asyncio.gather(producer_task, consumer_task)

    async def shutdown(self):
        self.running = False
        if self.drain_delay_on_term > 0:
            await asyncio.sleep(self.drain_delay_on_term)
        await self.writer.close_async()


def main():
    parser = argparse.ArgumentParser(description="Synthetic Tick Streamer for Soak/Chaos Tests")
    parser.add_argument("--lake-root", required=True, help="Path to tick lake root")
    parser.add_argument("--writer-id", default="streamer_synthetic", help="Writer ID for LakePublisher")
    parser.add_argument("--batch-size", type=int, default=50, help="Batch size for writing")
    parser.add_argument("--rate", type=float, default=200.0, help="Ticks per second")
    parser.add_argument("--fail-immediately", action="store_true", help="Exit immediately with code 1 for flapping tests")
    parser.add_argument("--drain-delay", type=float, default=0.5, help="Delay during graceful drain")

    args = parser.parse_args()

    if args.fail_immediately:
        sys.exit(1)

    lake_root = Path(args.lake_root)
    streamer = SyntheticStreamer(
        lake_root=lake_root,
        writer_id=args.writer_id,
        batch_size=args.batch_size,
        ticks_per_second=args.rate,
        drain_delay_on_term=args.drain_delay,
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    stop_event = asyncio.Event()

    def _sig_handler():
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _sig_handler)
        except NotImplementedError:
            signal.signal(sig, lambda s, f: stop_event.set())

    async def _runner():
        stream_task = asyncio.create_task(streamer.run())
        await stop_event.wait()
        stream_task.cancel()
        try:
            await stream_task
        except asyncio.CancelledError:
            pass
        await streamer.shutdown()

    try:
        loop.run_until_complete(_runner())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
