"""
Contract tests for the combined deterministic dataset (Q03 / PERF-01, ISOL-03).

The large-scale benchmarks are only meaningful if their input is reproducible,
bounded in memory, and actually contains the awkward rows the lake must preserve.
These tests pin all three properties.
"""

from collections import Counter

import pytest

from src.storage.config import decode_symbol, encode_symbol
from tests.support.deterministic_dataset import DEFAULT_SYMBOLS, DeterministicDataset


def test_same_seed_reproduces_identical_rows():
    a = DeterministicDataset(row_count=500, seed=7).generate()
    b = DeterministicDataset(row_count=500, seed=7).generate()
    assert a == b


def test_different_seed_produces_different_rows():
    a = DeterministicDataset(row_count=500, seed=7).generate()
    b = DeterministicDataset(row_count=500, seed=8).generate()
    assert a != b


def test_row_count_is_exact():
    assert len(DeterministicDataset(row_count=1_234, seed=1).generate()) == 1_234


def test_ingest_ids_are_stable_and_unique():
    ids = [t.ingest_id for t in DeterministicDataset(row_count=2_000, seed=3).generate()]
    assert len(set(ids)) == len(ids), "ingest_id values must be unique"
    assert all(i.startswith("ds3_") for i in ids), "ingest_id must derive from the seed"


def test_batch_boundaries_and_partial_final_batch():
    dataset = DeterministicDataset(row_count=250, seed=5)
    batches = list(dataset.iter_batches(batch_size=100))
    assert [len(b) for b in batches] == [100, 100, 50]
    assert sum(len(b) for b in batches) == 250


def test_iter_batches_is_lazy_and_bounded():
    """A generator, not a materialized list — this is the memory bound."""
    dataset = DeterministicDataset(row_count=10_000, seed=9)
    stream = dataset.iter_batches(batch_size=100)
    assert hasattr(stream, "__next__"), "iter_batches must return a lazy iterator"
    first = next(stream)
    assert len(first) == 100


@pytest.mark.parametrize(
    "attribute",
    ["duplicate_rows", "tied_timestamp_rows", "null_volume_rows", "zero_volume_rows", "late_arrival_rows"],
)
def test_every_edge_case_is_present(attribute):
    dataset = DeterministicDataset(row_count=5_000, seed=11)
    dataset.generate()
    assert getattr(dataset.manifest, attribute) > 0, f"{attribute} never occurred — dataset is not representative"


def test_dataset_includes_symbols_requiring_encoding():
    rows = DeterministicDataset(row_count=5_000, seed=13).generate()
    symbols = {t.symbol for t in rows}
    changed = {s for s in symbols if encode_symbol(s) != s}
    assert changed, f"no symbol required percent-encoding: {sorted(symbols)}"
    for symbol in changed:
        assert decode_symbol(encode_symbol(symbol)) == symbol, f"{symbol} does not round-trip"


def test_session_boundaries_are_exercised():
    dataset = DeterministicDataset(row_count=5_000, seed=17)
    dataset.generate()
    sessions = set(dataset.manifest.session_counts)
    assert {"PRE", "REG", "POST"} <= sessions, f"missing session boundaries: {sorted(sessions)}"


def test_hot_symbol_skew_is_present():
    dataset = DeterministicDataset(row_count=5_000, seed=19)
    dataset.generate()
    counts = dataset.manifest.symbol_counts
    hot = counts[DEFAULT_SYMBOLS[0]]
    others = [v for k, v in counts.items() if k != DEFAULT_SYMBOLS[0]]
    assert hot > max(others), f"hot symbol should dominate: {counts}"


def test_manifest_counts_match_the_generated_rows():
    dataset = DeterministicDataset(row_count=3_000, seed=23)
    rows = dataset.generate()
    manifest = dataset.manifest

    assert manifest.produced_rows == len(rows) == 3_000
    assert sum(manifest.symbol_counts.values()) == len(rows)
    assert sum(manifest.session_counts.values()) == len(rows)

    actual_symbols = Counter(t.symbol for t in rows)
    assert manifest.symbol_counts == dict(actual_symbols)

    actual_sessions = Counter(t.session for t in rows)
    assert manifest.session_counts == dict(actual_sessions)

    assert manifest.null_volume_rows == sum(1 for t in rows if t.volume is None)
    assert manifest.zero_volume_rows == sum(1 for t in rows if t.volume == 0.0)


def test_manifest_resets_between_passes():
    dataset = DeterministicDataset(row_count=1_000, seed=29)
    dataset.generate()
    first = dataset.manifest.produced_rows
    dataset.generate()
    assert dataset.manifest.produced_rows == first, "manifest must not accumulate across passes"


def test_partition_count_reflects_symbol_date_pairs():
    dataset = DeterministicDataset(row_count=4_000, seed=31)
    rows = dataset.generate()
    expected = len({(t.symbol, t.timestamp.date()) for t in rows})
    assert dataset.partition_count() == expected


def test_invalid_arguments_are_rejected():
    with pytest.raises(ValueError):
        DeterministicDataset(row_count=0)
    with pytest.raises(ValueError):
        list(DeterministicDataset(row_count=10).iter_batches(batch_size=0))


def test_perf_01_default_symbols_has_at_least_19_symbols():
    assert len(DEFAULT_SYMBOLS) >= 19
    assert len(set(DEFAULT_SYMBOLS)) == len(DEFAULT_SYMBOLS)


def test_perf_01_month_window_and_session_window_configurations():
    # Month window (default)
    month_ds = DeterministicDataset(row_count=1_000, seed=42, window_type="month")
    month_ds.generate()
    span = month_ds.manifest.date_spans
    assert span["days"] >= 28.0

    # Session window
    session_ds = DeterministicDataset(row_count=1_000, seed=42, window_type="session")
    session_ds.generate()
    s_span = session_ds.manifest.date_spans
    assert s_span["duration_seconds"] <= 12 * 3600


def test_perf_01_zipfian_skew_distribution_monotonicity():
    dataset = DeterministicDataset(row_count=10_000, seed=123)
    dataset.generate()
    counts = dataset.manifest.symbol_counts
    # Top 3 symbols should have more ticks than bottom 10 symbols
    top_3 = sum(counts[DEFAULT_SYMBOLS[i]] for i in range(3))
    bottom_10 = sum(counts[DEFAULT_SYMBOLS[-i]] for i in range(1, 11))
    assert top_3 > bottom_10
    # Every symbol should have received at least some ticks
    assert len(counts) == len(DEFAULT_SYMBOLS)


def test_perf_01_manifest_persistence_and_distributions(tmp_path):
    dataset = DeterministicDataset(row_count=1_000, seed=77)
    dataset.generate()
    
    # Save manifest
    target = tmp_path / "manifest.json"
    dataset.manifest.save_manifest(target)
    assert target.is_file()
    
    import json
    data = json.loads(target.read_text(encoding="utf-8"))
    assert data["seed"] == 77
    assert data["row_counts"]["produced"] == 1_000
    assert len(data["symbols"]) >= 19
    assert "date_spans" in data
    assert "partition_distribution" in data
    assert len(data["partition_distribution"]) > 0

