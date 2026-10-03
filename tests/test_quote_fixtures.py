"""
Unit tests for deterministic quote fixtures and pure Python reference oracle.
Verifies:
1. Conformance of all 7 edge-case scenarios to Lake Schema v1.
2. Exact duplicate preservation, sub-second bursts, late arrivals, nulls,
   midnight rollover, session boundaries, and multi-symbol skew.
3. Deterministic calculation of OHLCV candles, tie-breaking by (timestamp, ingest_id),
   volume coalescing semantics, and multi-timeframe aggregation.
"""
from datetime import datetime, date, timedelta, timezone
import pytest

from tests.fixtures.deterministic_quotes import (
    QuoteTick,
    ExpectedCandle,
    validate_quote_schema_v1,
    generate_exact_duplicates,
    generate_subsecond_burst,
    generate_out_of_order_late_arrivals,
    generate_null_and_capital_quotes,
    generate_utc_midnight_rollover,
    generate_nyse_session_and_dst,
    generate_multisymbol_distribution,
    get_scenario_quotes,
    calculate_expected_candles,
)


class TestDeterministicQuoteScenarios:
    """Verifies all 7 edge-case quote scenarios satisfy Lake Schema v1 and their domain contracts."""

    def test_scenario_exact_duplicates(self):
        """Scenario 1: Identical timestamps and prices must be preserved with distinct ingest_ids."""
        ticks = generate_exact_duplicates("AAPL")
        assert len(ticks) >= 8

        # 1. Validate Schema v1 conformity
        for tick in ticks:
            assert validate_quote_schema_v1(tick) is True

        # 2. Check for duplicate groups
        timestamp_counts = {}
        for tick in ticks:
            timestamp_counts[tick.timestamp] = timestamp_counts.get(tick.timestamp, 0) + 1

        duplicate_timestamps = [ts for ts, count in timestamp_counts.items() if count > 1]
        assert len(duplicate_timestamps) >= 2, "Expected at least 2 distinct duplicate timestamp clusters"

        # Check duplicate group details: same price, same volume, different ingest_id
        for dup_ts in duplicate_timestamps:
            group = [t for t in ticks if t.timestamp == dup_ts]
            prices = {t.price for t in group}
            assert len(prices) == 1, "Duplicate group should have identical price"
            ingest_ids = [t.ingest_id for t in group]
            assert len(ingest_ids) == len(set(ingest_ids)), "Every duplicate tick must have a distinct ingest_id"

    def test_scenario_subsecond_burst(self):
        """Scenario 2: High-density microsecond-spaced ticks within a single second."""
        ticks = generate_subsecond_burst("NVDA", count=50)
        assert len(ticks) == 50

        # 1. Validate Schema v1 conformity
        for tick in ticks:
            assert validate_quote_schema_v1(tick) is True

        # 2. Verify all ticks are within the exact same second
        first_sec = ticks[0].timestamp.replace(microsecond=0)
        for tick in ticks:
            assert tick.timestamp.replace(microsecond=0) == first_sec

        # 3. Verify microsecond progression
        microsecond_deltas = [
            (ticks[i].timestamp - ticks[i - 1].timestamp).total_seconds()
            for i in range(1, len(ticks))
        ]
        assert all(delta > 0 for delta in microsecond_deltas), "Burst timestamps should be non-decreasing"
        assert all(delta < 0.1 for delta in microsecond_deltas), "Microsecond deltas should be < 100ms"

    def test_scenario_out_of_order_late_arrivals(self):
        """Scenario 3: Sequence arrival order differs from event timestamp order."""
        ticks = generate_out_of_order_late_arrivals("MSFT")
        assert len(ticks) >= 8

        # 1. Validate Schema v1 conformity
        for tick in ticks:
            assert validate_quote_schema_v1(tick) is True

        # 2. Verify physical sequence is not sorted by timestamp
        is_sorted = all(ticks[i].timestamp <= ticks[i + 1].timestamp for i in range(len(ticks) - 1))
        assert not is_sorted, "Arrival sequence must contain out-of-order timestamps"

        # 3. Verify late arrivals arrive after ticks with later timestamps
        t_max_before_late = max(t.timestamp for t in ticks[:6])
        assert ticks[-1].timestamp < t_max_before_late, "Late arrival should have timestamp older than prior ticks"

    def test_scenario_nulls_and_capital_quotes(self):
        """Scenario 4: Nullable field handling and Capital volume=1.0 observation semantics."""
        ticks = generate_null_and_capital_quotes("AAPL")
        assert len(ticks) >= 5

        # 1. Validate Schema v1 conformity
        for tick in ticks:
            assert validate_quote_schema_v1(tick) is True

        # 2. Check Capital quote observation tick
        cap_quotes = [t for t in ticks if t.source == "CAPITAL" and t.volume == 1.0]
        assert len(cap_quotes) >= 1
        assert cap_quotes[0].bid is not None
        assert cap_quotes[0].ask is not None

        # 3. Check tick with nullable volume
        null_vol_ticks = [t for t in ticks if t.volume is None]
        assert len(null_vol_ticks) >= 2

        # 4. Check tick with all nullable fields None
        minimal_ticks = [
            t for t in ticks
            if t.volume is None and t.bid is None and t.ask is None and t.source is None and t.session is None
        ]
        assert len(minimal_ticks) >= 1

    def test_scenario_utc_midnight_rollover(self):
        """Scenario 5: Ticks spanning 23:59:59.999999 -> 00:00:00.000000 across UTC day boundary."""
        ticks = generate_utc_midnight_rollover("NVDA")
        assert len(ticks) >= 7

        # 1. Validate Schema v1 conformity
        for tick in ticks:
            assert validate_quote_schema_v1(tick) is True

        # 2. Verify ticks span two consecutive dates
        dates = {t.timestamp.date() for t in ticks}
        assert len(dates) == 2, f"Expected 2 dates, got {dates}"
        d1, d2 = sorted(dates)
        assert (d2 - d1).days == 1

        # 3. Verify exact microsecond edge ticks
        day1_edge = [t for t in ticks if t.timestamp.time() == datetime.strptime("23:59:59.999999", "%H:%M:%S.%f").time()]
        day2_edge = [t for t in ticks if t.timestamp.time() == datetime.strptime("00:00:00.000000", "%H:%M:%S.%f").time()]
        assert len(day1_edge) == 1, "Expected tick at 23:59:59.999999"
        assert len(day2_edge) == 1, "Expected tick at 00:00:00.000000"

    def test_scenario_nyse_session_and_dst(self):
        """Scenario 6: NYSE 09:30 open, 16:00 close edges and DST winter/summer shifts."""
        ticks = generate_nyse_session_and_dst("AAPL")
        assert len(ticks) >= 7

        # 1. Validate Schema v1 conformity
        for tick in ticks:
            assert validate_quote_schema_v1(tick) is True

        # 2. Check session values
        sessions = {t.session for t in ticks}
        assert "PRE" in sessions
        assert "REG" in sessions
        assert "POST" in sessions

        # 3. Check NYSE summer open edge (13:30 UTC for 09:30 EDT)
        summer_open_pre = [t for t in ticks if t.ingest_id == "nyse_open_pre_edge"]
        summer_open_reg = [t for t in ticks if t.ingest_id == "nyse_open_reg_edge"]
        assert summer_open_pre[0].session == "PRE"
        assert summer_open_reg[0].session == "REG"
        assert summer_open_reg[0].timestamp.time().hour == 13
        assert summer_open_reg[0].timestamp.time().minute == 30

        # 4. Check NYSE winter open edge (14:30 UTC for 09:30 EST)
        winter_open_reg = [t for t in ticks if t.ingest_id == "dst_winter_open"]
        assert winter_open_reg[0].timestamp.time().hour == 14
        assert winter_open_reg[0].timestamp.time().minute == 30

    def test_scenario_multisymbol_distribution(self):
        """Scenario 7: Multi-symbol realistic distribution (NVDA 60%, AAPL/MSFT 30%, ADBE/APP 10%)."""
        total = 1000
        ticks = generate_multisymbol_distribution(total_ticks=total)
        assert len(ticks) == total

        # 1. Validate Schema v1 conformity
        for tick in ticks:
            assert validate_quote_schema_v1(tick) is True

        # 2. Count per symbol
        counts = {}
        for t in ticks:
            counts[t.symbol] = counts.get(t.symbol, 0) + 1

        assert set(counts.keys()) == {"NVDA", "AAPL", "MSFT", "ADBE", "APP"}

        # 3. Check proportions
        nvda_pct = counts["NVDA"] / total
        assert 0.58 <= nvda_pct <= 0.62, f"NVDA percentage {nvda_pct} should be ~60%"

        top_tier = (counts["AAPL"] + counts["MSFT"]) / total
        assert 0.28 <= top_tier <= 0.32, f"AAPL+MSFT percentage {top_tier} should be ~30%"

        low_tier = (counts["ADBE"] + counts["APP"]) / total
        assert 0.08 <= low_tier <= 0.12, f"ADBE+APP percentage {low_tier} should be ~10%"

    def test_get_scenario_quotes_factory(self):
        """Verifies convenience factory loads all valid scenarios and errors on unknown."""
        for name in [
            "exact_duplicates",
            "subsecond_burst",
            "out_of_order",
            "late_arrivals",
            "nulls_and_capital",
            "utc_midnight_rollover",
            "nyse_session_dst",
            "multisymbol_distribution",
        ]:
            data = get_scenario_quotes(name)
            assert len(data) > 0

        with pytest.raises(ValueError, match="Unknown scenario"):
            get_scenario_quotes("invalid_scenario_name")


class TestPythonCandleOracle:
    """Verifies pure Python reference oracle calculations against expected OHLCV contracts."""

    def test_calculate_expected_candles_basic_ohlcv(self):
        """Computes deterministic OHLCV for 4 ticks within 1 minute."""
        base = datetime(2026, 10, 2, 14, 30, 0)
        ticks = [
            QuoteTick(base + timedelta(seconds=5), "AAPL", 150.0, 10.0, ingest_id="id_1"),
            QuoteTick(base + timedelta(seconds=15), "AAPL", 152.0, 5.0, ingest_id="id_2"),
            QuoteTick(base + timedelta(seconds=25), "AAPL", 149.0, 15.0, ingest_id="id_3"),
            QuoteTick(base + timedelta(seconds=45), "AAPL", 151.0, 20.0, ingest_id="id_4"),
        ]

        candles = calculate_expected_candles(ticks, timeframe="1m")
        assert len(candles) == 1
        c = candles[0]

        assert c.time == datetime(2026, 10, 2, 14, 30, 0)
        assert c.symbol == "AAPL"
        assert c.open == 150.0   # first tick price
        assert c.high == 152.0   # max price
        assert c.low == 149.0    # min price
        assert c.close == 151.0  # last tick price
        assert c.volume == 50.0  # 10 + 5 + 15 + 20
        assert c.tick_count == 4

        # Dictionary access compatibility
        assert c["open"] == 150.0
        assert c.to_dict()["close"] == 151.0

    def test_calculate_expected_candles_duplicate_timestamp_tiebreak(self):
        """
        Tie-breaking rule: When timestamps are identical, tie is broken by ingest_id ASC.
        The tick with MIN(timestamp, ingest_id) determines open.
        The tick with MAX(timestamp, ingest_id) determines close.
        """
        exact_ts = datetime(2026, 10, 2, 14, 30, 0, 500000)
        ticks = [
            QuoteTick(exact_ts, "NVDA", 125.0, 10.0, ingest_id="ingest_b_high"),
            QuoteTick(exact_ts, "NVDA", 120.0, 10.0, ingest_id="ingest_a_low"),
        ]

        candles = calculate_expected_candles(ticks, timeframe="1m")
        assert len(candles) == 1
        c = candles[0]

        # 'ingest_a_low' < 'ingest_b_high'
        assert c.open == 120.0, "Open should come from lexicographically earlier ingest_id"
        assert c.close == 125.0, "Close should come from lexicographically later ingest_id"
        assert c.high == 125.0
        assert c.low == 120.0
        assert c.volume == 20.0
        assert c.tick_count == 2

    def test_calculate_expected_candles_out_of_order_invariance(self):
        """Candles must be identical regardless of input tick arrival ordering."""
        base = datetime(2026, 10, 2, 14, 30, 0)
        ordered_ticks = [
            QuoteTick(base + timedelta(seconds=10), "MSFT", 400.0, 1.0, ingest_id="id_1"),
            QuoteTick(base + timedelta(seconds=20), "MSFT", 405.0, 1.0, ingest_id="id_2"),
            QuoteTick(base + timedelta(seconds=30), "MSFT", 398.0, 1.0, ingest_id="id_3"),
            QuoteTick(base + timedelta(seconds=40), "MSFT", 402.0, 1.0, ingest_id="id_4"),
        ]
        shuffled_ticks = [
            ordered_ticks[2],
            ordered_ticks[0],
            ordered_ticks[3],
            ordered_ticks[1],
        ]

        candles_ordered = calculate_expected_candles(ordered_ticks, timeframe="1m")
        candles_shuffled = calculate_expected_candles(shuffled_ticks, timeframe="1m")

        assert len(candles_ordered) == 1
        assert len(candles_shuffled) == 1

        co = candles_ordered[0]
        cs = candles_shuffled[0]

        assert co.open == cs.open == 400.0
        assert co.high == cs.high == 405.0
        assert co.low == cs.low == 398.0
        assert co.close == cs.close == 402.0
        assert co.volume == cs.volume == 4.0
        assert co.tick_count == cs.tick_count == 4

    def test_calculate_expected_candles_null_volume_semantics(self):
        """Null volume must coalesce to 1.0 per Capital quote observation semantics."""
        base = datetime(2026, 10, 2, 14, 30, 0)
        ticks = [
            QuoteTick(base + timedelta(seconds=1), "AAPL", 150.0, volume=None, ingest_id="n1"),
            QuoteTick(base + timedelta(seconds=2), "AAPL", 150.5, volume=None, ingest_id="n2"),
            QuoteTick(base + timedelta(seconds=3), "AAPL", 151.0, volume=10.0, ingest_id="v1"),
            QuoteTick(base + timedelta(seconds=4), "AAPL", 150.8, volume=None, ingest_id="n3"),
        ]

        candles = calculate_expected_candles(ticks, timeframe="1m")
        assert len(candles) == 1
        # 3 null volumes coalesce to 1.0 each (3.0) + 10.0 = 13.0
        assert candles[0].volume == 13.0
        assert candles[0].tick_count == 4

    def test_calculate_expected_candles_timeframes(self):
        """Tests bucket floor mathematics across multiple standard timeframes."""
        base = datetime(2026, 10, 2, 14, 33, 42, 123456)
        ticks = [QuoteTick(base, "AAPL", 150.0, 1.0, ingest_id="t1")]

        # 1s timeframe
        c_1s = calculate_expected_candles(ticks, timeframe="1s")[0]
        assert c_1s.time == datetime(2026, 10, 2, 14, 33, 42)

        # 5s timeframe (42 // 5 * 5 = 40)
        c_5s = calculate_expected_candles(ticks, timeframe="5s")[0]
        assert c_5s.time == datetime(2026, 10, 2, 14, 33, 40)

        # 1m timeframe
        c_1m = calculate_expected_candles(ticks, timeframe="1m")[0]
        assert c_1m.time == datetime(2026, 10, 2, 14, 33, 0)

        # 5m timeframe (33 // 5 * 5 = 30)
        c_5m = calculate_expected_candles(ticks, timeframe="5m")[0]
        assert c_5m.time == datetime(2026, 10, 2, 14, 30, 0)

        # 1h timeframe
        c_1h = calculate_expected_candles(ticks, timeframe="1h")[0]
        assert c_1h.time == datetime(2026, 10, 2, 14, 0, 0)

        # 1d timeframe
        c_1d = calculate_expected_candles(ticks, timeframe="1d")[0]
        assert c_1d.time == datetime(2026, 10, 2, 0, 0, 0)

    def test_calculate_expected_candles_multisymbol(self):
        """Multiple symbols in the same window must generate independent candles."""
        base = datetime(2026, 10, 2, 14, 30, 0)
        ticks = [
            QuoteTick(base + timedelta(seconds=1), "NVDA", 120.0, 1.0, ingest_id="n1"),
            QuoteTick(base + timedelta(seconds=2), "AAPL", 150.0, 1.0, ingest_id="a1"),
            QuoteTick(base + timedelta(seconds=3), "NVDA", 121.0, 1.0, ingest_id="n2"),
            QuoteTick(base + timedelta(seconds=4), "AAPL", 151.0, 1.0, ingest_id="a2"),
        ]

        candles = calculate_expected_candles(ticks, timeframe="1m")
        assert len(candles) == 2
        sym_map = {c.symbol: c for c in candles}

        assert sym_map["NVDA"].open == 120.0
        assert sym_map["NVDA"].close == 121.0
        assert sym_map["NVDA"].tick_count == 2

        assert sym_map["AAPL"].open == 150.0
        assert sym_map["AAPL"].close == 151.0
        assert sym_map["AAPL"].tick_count == 2

    def test_calculate_expected_candles_empty(self):
        """Empty input returns empty list."""
        assert calculate_expected_candles([], timeframe="1m") == []
