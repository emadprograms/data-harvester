# Phase 6 Plan: Capital.com Exclusive WebSocket Streamer & Dynamic Reload

**Goal:** Refactor the live streaming runner to stream raw tick quotes exclusively from Capital.com, deactivate Binance from the active runner, and implement dynamic live reload of symbol subscriptions without restarting the daemon.

**Requirements:** STRM-05, STRM-06, STRM-07

## Tasks

### Task 1: CapitalStreamer Subscription Management (`src/stream/capital_stream.py`)
- Maintain active WebSocket connection reference (`self.ws`).
- Implement `async def update_subscriptions(self, new_epics: list[str])`:
  - Calculate added and removed epics.
  - Send `marketData.unsubscribe` for removed epics over active WebSocket.
  - Send `marketData.subscribe` for newly added epics.
  - Update `self.epics` in place without dropping connection or re-authenticating.

### Task 2: StreamingEngine Capital.com Exclusivity & Dynamic Reload (`src/stream/runner.py`)
- Deactivate Binance streamer from `StreamingEngine.start()` — stream exclusively from Capital.com.
- Maintain `_handle_binance_tick` solely for backward compatibility with existing tests.
- Target `DEFAULT_STREAMING_DB_PATH` (`data/streaming.duckdb`).
- Implement dynamic reload:
  - `async def reload_symbols(self)`: Re-queries `symbol_map` from historical DB, extracts Capital tickers, and invokes `capital_streamer.update_subscriptions()`.
  - Provide reload trigger method `trigger_reload()` and background polling watcher for symbol inventory updates.

### Task 3: Comprehensive Test Suite (`tests/test_stream_runner_capital_exclusive_dynamic_reload.py`)
- Test Capital.com exclusive streaming engine startup (verifying Binance streamer is not started by default).
- Test dynamic symbol addition and removal in `CapitalStreamer`:
  - Mock WebSocket `send` to verify `marketData.subscribe` and `marketData.unsubscribe` payloads.
- Test `StreamingEngine.reload_symbols()` end-to-end with mock database `symbol_map`.
- Test raw tick quote writing into `data/streaming.duckdb`.
- Test continuous stream health and reconnection backoff.

### Task 4: Verification & Regression Testing
- Run full pytest test suite to ensure all existing and new streaming tests pass 100%.
- Produce `06-SUMMARY.md`.
