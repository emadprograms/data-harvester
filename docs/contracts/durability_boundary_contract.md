# Durability Boundary Contract & Honesty Guarantee

**Release Milestone:** 4.3 (Package D: Honest Durability Boundaries & Provider Gap Ledger)  
**Status:** Canonical & Enforced  
**Verification Suite:** `tests/storage/test_durability_boundary.py`, `tests/storage/test_durability_faults.py`, `tests/stream/test_lake_runner_stress.py`, `tests/stream/test_gap_ledger.py`

---

## 1. Executive Guarantee Statement

> **"Durable tick spooling and provider replay are optional; this release guarantees the documented RAM loss boundary."**

Data Harvester never claims zero power-loss or provider-wide zero loss without a durable inbox / disk spool. The boundaries of durability are clearly defined, observable, and verified across all persistence stages.

---

## 2. Four Durability Outcomes Across Lifecycle Events

| Outcome | Trigger / Condition | Durability Guarantee | Lake Integrity |
|---|---|---|---|
| **Durable (Committed)** | Batch publication completed with atomic receipt written to `_control/receipts/<batch_id>.json` and fsynced. | **Guaranteed Durable.** Survives OS SIGKILL, power outage, process crashes, and machine reboots. Recovered idempotently. | 100% valid footers, valid schema, no torn records. |
| **RAM-Only (Unflushed)** | Ticks admitted to memory buffers (`write_queue` or writer buffer) but not yet published or receipt-fsynced. | **Lost upon Hard Crash.** When an ungraceful crash (SIGKILL, hardware power-cut, OOM killer) occurs, unflushed RAM ticks are lost. | Lake remains fully valid. Uncommitted partial staging files are reclaimed or completed idempotently on next recovery. |
| **Graceful Drain** | Clean shutdown initiated via real OS signals (`SIGINT`, `SIGTERM`) or engine `shutdown()`. | **All Admitted Ticks Made Durable.** `_shutdown_signal_handler` catches signal, stops admission, drains buffered and in-flight ticks to Parquet lake, updates status to `STOPPED`, and terminates with returncode 0. | All admitted ticks committed to Parquet. Zero temporary files left in `_staging/`. |
| **Failed Publication** | Storage failure (ENOSPC, EIO, disk full, or barrier fault) outlasting retry attempts. | **Pending Work Honest Failure.** Pending work is never reported committed or healthy-drained. The writer status transitions to `DRAIN_FAILED` or reports error with exit code 4. | Previous publications remain unchanged. No uncommitted partial batches appear in receipt inventory. |

---

## 3. Seven Persistence Boundaries & Fault Handling

The persistence pipeline defines seven named barrier injection points:

1. **Admission (`admission`):** Enqueueing ticks into writer / engine buffer. If storage/memory rejected, tick is dropped and logged, not silently acknowledged.
2. **Intent Durability (`intent_durability`):** Writing and fsyncing `_control/intent/<batch_id>.json`. Survives failures via retry; unrecovered intents are safely reconciled upon restart.
3. **Staged-File Fsync (`staged_fsync`):** Calling `os.fsync` on staged Parquet files in `_staging/`. Unfsynced data is never promoted.
4. **Staged-File Promotion (`staged_promotion`):** Atomic rename (`os.replace`) from `_staging/` to `ticks/symbol=*/date=*/*.parquet`.
5. **Directory Fsync (`directory_fsync`):** Calling `os.fsync` on the partition directory file descriptor on POSIX to persist the directory entry. Real I/O errors (`ENOSPC`, `EIO`) bubble up and trigger writer retries.
6. **Receipt Durability (`receipt_durability`):** Writing, fsyncing, and atomically renaming `_control/receipts/<batch_id>.json`. This is the exact atomic commitment barrier.
7. **Acknowledgment (`acknowledgment`):** Returning receipt confirmation to the caller / worker loop. Transient failures replay the existing durable receipt without duplicating rows.

### Fault Matrix Behaviors
- **Transient Faults (1 failure):** Absorbed by writer exponential backoff retry. Batch is published once with exact row multiset (`EXCEPT ALL` matches oracle).
- **Persistent Faults (outlasting retries):** Exception raised, error recorded in `writer_status.json`, `total_published` is not incremented, and no batch receipt is created.

---

## 4. Multi-Partition Atomic Batch Reconciliation

When a publication batch spans multiple partition directories (e.g. multiple symbols or crossing UTC midnight dates):
- If a crash occurs after only *some* partition files have been promoted to `ticks/...`, glob readers may see partial files before recovery, but the publication receipt is **not** written.
- Upon process restart, `recover_pending_publications` examines the intent file, validates checksums and row counts of all targets (both already promoted and still staged), promotes the remaining staged files, verifies all files, writes the receipt, and removes the intent file.
- Recovery is **strictly idempotent**: repeated recovery passes produce the identical durable row set with zero duplicates or data loss.

---

## 5. Provider Gap Ledger & Disconnect Accounting

Capture gaps and provider disconnects are tracked in `<lake_root>/_control/gaps.json`.

### Gap Entry Contract
- `gap_id`: Unique identifier for the gap incident.
- `provider` / `source`: Provider or streamer identity (e.g. `CAPITAL`, `BINANCE`, `MOCK_CAPITAL`).
- `symbol`: Ticker symbol or `"all"`.
- `start_time`: UTC ISO8601 timestamp when gap began.
- `end_time`: UTC ISO8601 timestamp when gap ended (or `None` if ongoing).
- `reason`: Classification (e.g. `DISCONNECT`, `BUFFER_OVERFLOW`, `RATE_LIMIT`, `SUPERVISOR_HANDOFF`, `SHUTDOWN_UNFLUSHED`).
- `status`: **`"LOSS_UNKNOWN"`** when exact count of unreceived provider ticks cannot be known.

### Honesty Rule
> **Never invent a fake count of lost ticks during disconnects.**
> When disconnected from a live non-replayable WebSocket, ticks emitted by the exchange cannot be known with certainty. The ledger records `status: "LOSS_UNKNOWN"` rather than fabricating estimates. Replay-capable feeds replay missed sequences upon reconnection.

---

## 6. Verification Traceability

| Requirement | Test Location | Test Description |
|---|---|---|
| **DURB-01** | `tests/stream/test_lake_runner_stress.py`<br>`tests/storage/test_durability_boundary.py` | Real OS subprocess running `python -m src.stream.runner` handling `SIGINT` and `SIGTERM`. Verifies `_shutdown_signal_handler`, clean queue drain, `STOPPED` status, exit code 0. |
| **DURB-02** | `tests/storage/test_durability_faults.py`<br>`tests/storage/test_durability_boundary.py` | Named barrier injection across all 7 boundaries, parameterized transient/persistent ENOSPC/EIO, multi-partition crash recovery with bidirectional multiset equality. |
| **DURB-03** | `tests/stream/test_gap_ledger.py`<br>`src/stream/gap_ledger.py`<br>`src/stream/fake_provider.py` | Provider gap ledger, `FakeProvider` with sequence ledger, disconnect/reconnect, buffer overflow drops, supervisor handoff, readable via `TickLakeReader.read_gaps()`. |
| **DURB-04** | `tests/storage/test_durability_boundary.py` | Contract documentation assertion and verification of RAM loss boundary guarantees. |
