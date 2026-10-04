"""
Subprocess writer for the durability boundary tests (Q05 / DURB-01, DURB-03, DURB-05).

Runs in its own process so it can be killed with SIGKILL at a precisely chosen
point — an in-process exception cannot simulate a power loss or OOM kill. The
worker writes an independent ledger of what the writer *acknowledged* before the
kill, which the test compares against what actually became durable on disk.

Modes:
  --ack-rows N   publish N rows (acknowledged/durable), admit the rest to RAM
                 only, signal readiness, then wait to be killed.
  --drain        publish every row, close cleanly, exit 0 (healthy stop).
  --fail-publish force publication to fail, report pending work, exit 4.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path


def _rows(seed: int, count: int):
    from tests.support.deterministic_dataset import DeterministicDataset

    return next(DeterministicDataset(row_count=count, seed=seed).iter_batches(batch_size=max(count, 1)))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lake-root", required=True)
    parser.add_argument("--seed", type=int, default=4242)
    parser.add_argument("--rows", type=int, default=400)
    parser.add_argument("--ack-rows", type=int, default=0, help="rows to publish before awaiting SIGKILL")
    parser.add_argument("--ledger", required=True)
    parser.add_argument("--ready", required=True)
    parser.add_argument("--drain", action="store_true", help="publish everything and exit cleanly")
    parser.add_argument("--fail-publish", action="store_true", help="force publication failure and exit 4")
    args = parser.parse_args(argv)

    from src.storage.parquet_writer import TickLakeWriter

    lake_root = Path(args.lake_root)
    rows = _rows(args.seed, args.rows)

    # A very large batch threshold and flush interval keep publication entirely
    # under the test's control: nothing publishes unless we call flush().
    writer = TickLakeWriter(
        root=lake_root,
        writer_id="durability_worker",
        max_batch_rows=10**9,
        flush_interval_seconds=10**6,
    )

    ledger: dict[str, object] = {
        "seed": args.seed,
        "rows_total": len(rows),
        "mode": "drain" if args.drain else ("fail_publish" if args.fail_publish else "crash"),
    }

    if args.fail_publish:
        def explode(*_a, **_k):
            raise OSError("simulated publication failure")

        writer.publisher.publish_batch = explode  # type: ignore[method-assign]
        writer.write_ticks(rows)
        pending = writer.metrics.total_accepted
        try:
            writer.flush(block=True)
            failure = None
        except Exception as exc:  # noqa: BLE001 - recording the error is the point
            failure = f"{type(exc).__name__}: {exc}"
        ledger.update(
            {
                "acknowledged": [],
                "pending": [t.ingest_id for t in rows],
                "pending_admitted": pending,
                "published_rows": writer.metrics.total_published,
                "failure": failure,
            }
        )
        Path(args.ledger).write_text(json.dumps(ledger), encoding="utf-8")
        Path(args.ready).write_text("failed", encoding="utf-8")
        writer.close()
        return 4

    ack_rows = len(rows) if args.drain else max(0, min(args.ack_rows, len(rows)))

    if ack_rows:
        writer.write_ticks(rows[:ack_rows])
        writer.flush(block=True)

    acknowledged = [t.ingest_id for t in rows[:ack_rows]]
    pending = [t.ingest_id for t in rows[ack_rows:]]

    if args.drain:
        if pending:
            writer.write_ticks(rows[ack_rows:])
            writer.flush(block=True)
            acknowledged = [t.ingest_id for t in rows]
            pending = []
        ledger["published_rows"] = writer.metrics.total_published
        ledger["acknowledged"] = acknowledged
        ledger["pending"] = pending
        Path(args.ledger).write_text(json.dumps(ledger), encoding="utf-8")
        Path(args.ready).write_text("drained", encoding="utf-8")
        writer.close()
        return 0

    # Admit the remainder to RAM only, then signal readiness and await SIGKILL.
    if pending:
        writer.write_ticks(rows[ack_rows:])

    ledger.update(
        {
            "acknowledged": acknowledged,
            "pending": pending,
            "admitted": writer.metrics.total_accepted,
            "published_rows": writer.metrics.total_published,
        }
    )
    Path(args.ledger).write_text(json.dumps(ledger), encoding="utf-8")
    Path(args.ready).write_text("ready", encoding="utf-8")

    # Hold the pending rows in RAM until the parent kills this process.
    time.sleep(120)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
