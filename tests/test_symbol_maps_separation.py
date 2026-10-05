"""
Symbol authority tests for the Parquet-only lake (v5.0).

There is exactly one symbol map: the lake registry (`_control/registry.json`).
Verifies:
1. The registry holds the 19 approved equities and nothing else (no ETFs, no crypto).
2. The stream runner drops every tick whose symbol is not in the active set.
3. The Databento backfiller targets the registry's approved equities.
"""
import asyncio
from datetime import datetime, timezone

from src.config import APPROVED_EQUITY_SYMBOLS
from tests.support.lake_population import create_lake
from src.stream.runner import StreamingEngine
from src.data.databento_backfill import get_target_stock_symbols


def test_lake_registry_holds_exactly_the_approved_equities(tmp_path):
    """The lake registry is the single symbol authority: the 19 equities, no ETFs or crypto."""
    lake = create_lake(tmp_path / "lake", symbols=APPROVED_EQUITY_SYMBOLS)
    from src.storage.registry import SymbolRegistry

    snapshot = SymbolRegistry(root=lake).load()
    symbols = sorted(snapshot.symbols)
    assert symbols == sorted(APPROVED_EQUITY_SYMBOLS)
    assert len(symbols) == 19

    # Pure stocks are present
    for expected in ["AAPL", "NVDA", "MSFT", "AMZN", "GOOGL", "TSLA", "AMD"]:
        assert expected in symbols

    # ETFs and crypto are strictly absent
    for excluded in ["SPY", "QQQ", "IWM", "DIA", "BTCUSDT", "ETHUSDT", "CL=F", "VIX"]:
        assert excluded not in symbols


def test_stream_runner_drops_excluded_assets(tmp_path):
    """Verify that StreamingEngine strictly drops ticks for assets not in the active symbol set."""
    async def _test():
        engine = StreamingEngine(lake_root=tmp_path / "lake")
        # Configure allowed symbols to only AAPL and NVDA
        engine.active_streaming_symbols = {"AAPL", "NVDA"}
        engine.epic_to_display = {"AAPL": "AAPL", "NVDA": "NVDA"}

        # Simulate ticks from Capital.com
        now = datetime.now(timezone.utc)

        # 1. Allowed tick: NVDA
        await engine._handle_capital_tick({"epic": "NVDA", "price": 225.5, "timestamp": now, "bid": 225.4, "ask": 225.6})

        # 2. Excluded tick: SPY (ETF)
        await engine._handle_capital_tick({"epic": "SPY", "price": 550.0, "timestamp": now, "bid": 549.9, "ask": 550.1})

        # 3. Excluded tick: US30 (Dow index)
        await engine._handle_capital_tick({"epic": "US30", "price": 42000.0, "timestamp": now, "bid": 41999.0, "ask": 42001.0})

        # 4. Allowed tick: AAPL
        await engine._handle_capital_tick({"epic": "AAPL", "price": 230.0, "timestamp": now, "bid": 229.9, "ask": 230.1})

        # Queue should only contain 2 ticks (NVDA, AAPL); SPY and US30 dropped!
        assert engine.write_queue.qsize() == 2

        tick1 = engine.write_queue.get_nowait()
        assert tick1[1] == "NVDA"

        tick2 = engine.write_queue.get_nowait()
        assert tick2[1] == "AAPL"

    asyncio.run(_test())


def test_databento_backfill_targets_lake_registry():
    """Verify get_target_stock_symbols reads the lake registry, scoped to the 19 approved equities."""
    symbols = get_target_stock_symbols()
    assert len(symbols) == 19
    assert "NVDA" in symbols
    assert "AAPL" in symbols
    assert "SPY" not in symbols
    assert "BTCUSDT" not in symbols
