---
quick_id: 260925-rel
date: 2026-09-25
status: complete
description: Rename all tests to test_(folder_name in src)_(filename inside the folder)_(what it tests).py
---

# Quick Task Summary: Rename All Tests

## Accomplishments
Renamed all 14 test files in `tests/` to follow the standardized semantic naming pattern:
`test_(folder_name_in_src)_(filename_inside_folder)_(what_it_tests).py`

### Mappings
1. `tests/test_binance.py` -> `tests/test_api_binance_rest_klines.py`
2. `tests/test_capital_detailed.py` -> `tests/test_api_capital_historical_fetch.py`
3. `tests/test_retry.py` -> `tests/test_api_retry_http_adapter.py`
4. `tests/test_config.py` -> `tests/test_src_config_constants.py`
5. `tests/test_credentials.py` -> `tests/test_src_credentials_env_loading.py`
6. `tests/test_main_smoke.py` -> `tests/test_root_main_cli_smoke.py`
7. `tests/test_date_logic.py` -> `tests/test_data_harvester_session_dates.py`
8. `tests/test_normalizer.py` -> `tests/test_data_normalizer_yahoo_ohlcv.py`
9. `tests/test_harvester.py` -> `tests/test_data_harvester_source_routing.py`
10. `tests/test_edge_cases.py` -> `tests/test_data_harvester_edge_cases.py`
11. `tests/test_integration.py` -> `tests/test_data_harvester_full_pipeline.py`
12. `tests/test_database.py` -> `tests/test_database_operations_mock_crud.py`
13. `tests/test_duckdb_operations.py` -> `tests/test_database_operations_duckdb_storage.py`
14. `tests/test_streaming.py` -> `tests/test_stream_runner_live_engine.py`

## Verification
Ran full test suite: **120 passed in 6.57s**, zero failures, zero warnings.
