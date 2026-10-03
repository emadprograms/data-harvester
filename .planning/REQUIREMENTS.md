# Requirements: Milestone v4.1

# Partitioned Parquet Lake Deep Testing & Hardening

## Milestone Goal

Harden and stress-test the partitioned Parquet tick lake architecture across extreme edge cases, high concurrency, adverse failure modes, fuzzing, and long-running multi-process chaos. Ensure ironclad data integrity, crash resilience, leak-free in-memory DuckDB scaling, race-free symbol registry mutations, and flawless migration under hostile operational conditions.

---

## Requirements

### Phase 22: Storage Foundation & Publication Edge Case Tests
- [x] **TEST-P22-01**: Storage layout path traversal, unicode/special symbol encoding, and corrupted metadata handling (`src/storage/config.py`).
- [x] **TEST-P22-02**: PyArrow schema type coercion, extreme numeric limits (float min/max, subnormal), null bitmasks (`src/storage/schema.py`).
- [x] **TEST-P22-03**: Atomic publication concurrency collisions, crashed intent recovery, and file lock serialization (`src/storage/publication.py`).

### Phase 23: Streaming Writer & Runner Stress & Lifecycle Tests
- [x] **TEST-P23-01**: High-throughput micro-batching under memory pressure (100k+ ticks) and bounded queue backpressure (`src/storage/parquet_writer.py`).
- [x] **TEST-P23-02**: Runner sudden shutdown mid-flush, graceful drain timeouts, and honest queue acknowledgments (`src/stream/runner.py`).
- [x] **TEST-P23-03**: Transient disk full / I/O error exponential backoff and quarantine handling (`src/storage/parquet_writer.py`).

### Phase 24: Versioned Symbol Registry & Dynamic Reload Stress Tests
- [x] **TEST-P24-01**: Cross-process concurrent symbol CRUD lock serialization and monotonic versioning integrity (`src/storage/registry.py`).
- [x] **TEST-P24-02**: Rapid symbol toggle/delete flapping and `PENDING_PURGE` generation fences (`src/storage/registry.py`).
- [x] **TEST-P24-03**: File signal debouncing and dynamic reload latency under heavy polling (`src/dashboard/server.py`).

### Phase 25: In-Memory DuckDB Lake Reader & Analytics Edge Tests
- [x] **TEST-P25-01**: Multi-threaded in-memory DuckDB connection scaling (30+ concurrent readers) without memory leaks (`src/storage/reader.py`).
- [x] **TEST-P25-02**: Vectorized resampling edge cases: sparse partitions, multi-day roll-overs, DST shifts, leap years (`src/storage/reader.py`, `src/dashboard/analytics.py`).
- [x] **TEST-P25-03**: Reverse-chronological tape pagination with high offsets and non-existent symbol pruning (`src/storage/reader.py`).

### Phase 26: Migration Tooling Rehearsal & Fuzz Tests
- [x] **TEST-P26-01**: Migration of corrupt / partial legacy DuckDB tables and schema drift (`tools/migrate_streaming_to_parquet.py`).
- [x] **TEST-P26-02**: Simulated crash interruption across all migration modes (`plan`, `export`, `verify`, `publish`) (`tools/migrate_streaming_to_parquet.py`).
- [x] **TEST-P26-03**: Two-way `EXCEPT ALL` fuzz testing with synthetic data corruption and precision mismatch detection (`tools/migrate_streaming_to_parquet.py`).

### Phase 27: Multi-Process Long-Running Soak & Chaos Tests
- [ ] **TEST-P27-01**: Multi-process soak testing under continuous ingestion and continuous analytical reading (`tools/service_supervisor.py`, `tools/validate_concurrency.py`).
- [ ] **TEST-P27-02**: Chaos monkey process termination (streamer, dashboard, supervisor) and automatic self-healing (`tools/service_supervisor.py`).

---

## Non-Functional Requirements & Performance Gates

- **Zero Memory Leaks**: In-memory DuckDB reader pools and worker threads release all query and buffer memory across 1,000+ sequential and 30+ concurrent calls.
- **Backpressure Stability**: Streaming runner bounded queues throttle or shed honestly under extreme tick surges without unhandled OOM.
- **Fail-Safe Crash Recovery**: Any crashed writer, uncommitted `.tmp` staging file, or partial migration batch is automatically quarantined or cleaned up without lake corruption.
- **Monotonic Registry State**: Symbol registry version strictly increments and rejects concurrent race conditions or torn writes across processes.
- **Self-Healing Supervisor**: Multi-process supervisor detects killed or crashing sub-processes and restores steady-state streaming within SLA.
