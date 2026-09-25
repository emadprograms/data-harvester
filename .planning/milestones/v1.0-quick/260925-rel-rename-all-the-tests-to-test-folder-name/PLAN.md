---
quick_id: 260925-rel
date: 2026-09-25
status: in_progress
description: Rename all tests to test_(folder_name in src)_(filename inside the folder)_(what it tests).py
---

# Quick Plan: Rename All Tests to Semantic Format

## Objective
Standardize test filenames across the repository according to the convention:
`test_(folder_name_in_src)_(filename_inside_folder)_(what_it_tests).py`

## Plan of Work
1. Rename all 14 test files using git mv:
   - `tests/test_binance.py` -> `tests/test_api_binance_rest_klines.py`
   - `tests/test_capital_detailed.py` -> `tests/test_api_capital_historical_fetch.py`
   - `tests/test_retry.py` -> `tests/test_api_retry_http_adapter.py`
   - `tests/test_config.py` -> `tests/test_src_config_constants.py`
   - `tests/test_credentials.py` -> `tests/test_src_credentials_env_loading.py`
   - `tests/test_main_smoke.py` -> `tests/test_root_main_cli_smoke.py`
   - `tests/test_date_logic.py` -> `tests/test_data_harvester_session_dates.py`
   - `tests/test_normalizer.py` -> `tests/test_data_normalizer_yahoo_ohlcv.py`
   - `tests/test_harvester.py` -> `tests/test_data_harvester_source_routing.py`
   - `tests/test_edge_cases.py` -> `tests/test_data_harvester_edge_cases.py`
   - `tests/test_integration.py` -> `tests/test_data_harvester_full_pipeline.py`
   - `tests/test_database.py` -> `tests/test_database_operations_mock_crud.py`
   - `tests/test_duckdb_operations.py` -> `tests/test_database_operations_duckdb_storage.py`
   - `tests/test_streaming.py` -> `tests/test_stream_runner_live_engine.py`

2. Run full pytest suite to verify all 120 tests pass with the new names.
3. Commit and push changes.
