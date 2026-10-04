"""
Contract tests for the deterministic quote generator (Q02 / ISOL-03).

The oracle is only as good as the dataset it compares against, so the generator's
guarantees are pinned here: byte-for-byte determinism across calls, stable unique
ingest_ids, and the presence of every edge case the lake is required to preserve
(timestamp ties, exact duplicate observations, null volume, late arrivals, UTC
date rollover, and session boundaries).
"""

from collections import Counter
from datetime import datetime

import pytest

from tests.fixtures.deterministic_quotes import (
    generate_exact_duplicates,
    generate_multisymbol_distribution,
    generate_null_and_capital_quotes,
    generate_nyse_session_and_dst,
    generate_out_of_order_late_arrivals,
    generate_subsecond_burst,
    generate_utc_midnight_rollover,
    validate_quote_schema_v1,
)

SCENARIOS = [
    generate_exact_duplicates,
    generate_subsecond_burst,
    generate_out_of_order_late_arrivals,
    generate_null_and_capital_quotes,
    generate_utc_midnight_rollover,
    generate_nyse_session_and_dst,
    generate_multisymbol_distribution,
]


@pytest.mark.parametrize("generator", SCENARIOS, ids=lambda g: g.__name__)
def test_generator_is_deterministic(generator):
    assert generator() == generator(), f"{generator.__name__} is not reproducible"


@pytest.mark.parametrize("generator", SCENARIOS, ids=lambda g: g.__name__)
def test_generator_emits_stable_unique_ingest_ids(generator):
    ids = [tick.ingest_id for tick in generator()]
    assert ids, "no ticks generated"
    assert all(isinstance(i, str) and i for i in ids), "ingest_id must be a non-empty string"
    assert len(set(ids)) == len(ids), f"duplicate ingest_id values: {ids}"


@pytest.mark.parametrize("generator", SCENARIOS, ids=lambda g: g.__name__)
def test_generator_output_satisfies_schema_v1(generator):
    for tick in generator():
        assert validate_quote_schema_v1(tick), f"schema violation in {generator.__name__}: {tick}"


@pytest.mark.parametrize("generator", SCENARIOS, ids=lambda g: g.__name__)
def test_generator_emits_utc_naive_timestamps(generator):
    for tick in generator():
        assert tick.timestamp.tzinfo is None, "timestamps must be UTC-naive"


def test_exact_duplicates_contain_a_pair_and_a_triplet():
    ticks = generate_exact_duplicates()
    groups = Counter(
        (t.timestamp, t.symbol, t.price, t.volume, t.bid, t.ask, t.source, t.session) for t in ticks
    )
    sizes = sorted((v for v in groups.values() if v > 1))
    assert sizes == [2, 2, 3], f"expected two pairs and one triplet, got {sizes}"


def test_subsecond_burst_uses_microsecond_resolution():
    ticks = generate_subsecond_burst(count=25)
    stamps = [t.timestamp for t in ticks]
    assert len(set(stamps)) == len(stamps), "burst timestamps must be distinct at microsecond resolution"
    assert any(s.microsecond for s in stamps)


def test_late_arrivals_are_out_of_order():
    stamps = [t.timestamp for t in generate_out_of_order_late_arrivals()]
    inversions = sum(1 for i in range(1, len(stamps)) if stamps[i] < stamps[i - 1])
    assert inversions >= 1, "late-arrival scenario contains no out-of-order timestamps"


def test_null_and_capital_quotes_include_null_observations():
    ticks = generate_null_and_capital_quotes()
    assert any(t.volume is None for t in ticks), "no null volume observations"
    assert any(t.bid is None for t in ticks), "no null bid observations"


def test_midnight_rollover_spans_two_utc_dates():
    dates = {t.timestamp.date() for t in generate_utc_midnight_rollover()}
    assert len(dates) == 2, f"rollover must cross a UTC date boundary, saw {sorted(dates)}"


def test_session_scenario_covers_pre_regular_and_post():
    sessions = {t.session for t in generate_nyse_session_and_dst()}
    assert {"PRE", "REG", "POST"} <= sessions, f"missing session boundaries: {sorted(sessions)}"


def test_multisymbol_distribution_is_skewed_and_stable_across_sizes():
    ticks = generate_multisymbol_distribution(total_ticks=500)
    counts = Counter(t.symbol for t in ticks)
    assert len(counts) >= 5, f"expected a multi-symbol mix, got {dict(counts)}"
    hottest = max(counts.values())
    coldest = min(counts.values())
    assert hottest > coldest, "distribution should be skewed, not uniform"
    assert sum(counts.values()) == 500

    smaller = generate_multisymbol_distribution(total_ticks=100)
    assert len(smaller) == 100
