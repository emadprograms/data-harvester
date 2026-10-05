"""Phase 47 guards — the bar era is gone, the lake era stays.

Every test here asserts an *absence* that the milestone requires, or the
survival of something the owner asked to keep. Written before the removals
(TDD), so they fail until Phase 47 is implemented.

Scope note: `tools/migrate_streaming_to_parquet.py` is deliberately exempt from
the "no .duckdb reference" checks. It is the single permitted disk-database
reader — the Phase 49 migration gate the owner runs before deleting the files.
"""
from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(relative: str) -> str:
    return (REPO_ROOT / relative).read_text(encoding="utf-8")


# ---------------------------------------------------------------- RMV-08 replay

REPLAY_FILES = (
    "src/storage/replay.py",
    "tests/storage/test_replay.py",
)

REPLAY_EXPORTS = (
    "ReplayError",
    "ReplaySnapshotInvalidError",
    "ReplaySnapshotRetiredError",
    "ReplayCursorCorruptedError",
    "ReplaySnapshot",
    "ReplayCursor",
    "TickLakeReplayIterator",
    "create_replay_snapshot",
)


def test_replay_files_are_deleted() -> None:
    for relative in REPLAY_FILES:
        assert not (REPO_ROOT / relative).exists(), f"replay file survived: {relative}"


def test_storage_package_exports_no_replay_symbols() -> None:
    source = _read("src/storage/__init__.py")
    for name in REPLAY_EXPORTS:
        assert name not in source, f"replay export remains in src/storage/__init__.py: {name}"


def test_tick_lake_reader_has_no_replay_methods() -> None:
    from src.storage.reader import TickLakeReader

    for name in ("create_replay_iterator", "create_replay_snapshot"):
        assert not hasattr(TickLakeReader, name), f"replay method survives: {name}"


def test_reader_still_exposes_its_read_surface() -> None:
    """The decks the dashboard and migration depend on must survive untouched."""
    from src.storage.reader import TickLakeReader

    for name in (
        "validate_lake",
        "query_candles",
        "get_candles",
        "query_ticks",
        "get_tape",
        "get_latest_tick",
        "get_stream_status",
        "read_gaps",
        "discover_available_weeks",
        "get_streaming_continuity_analysis",
        "get_lake_health_report",
    ):
        assert hasattr(TickLakeReader, name), f"read surface lost: {name}"


# --------------------------------------------------------------- RMV-09 discord

DISCORD_REMOVED = (
    "build_health_alerts",
    "build_database_health_grid",
    "send_discord_harvest_report",
)

DISCORD_RETAINED = (
    "_post_embed",
    "_post_file",
)


def test_discord_drops_bar_era_builders() -> None:
    source = _read("src/utils/discord.py")
    for name in DISCORD_REMOVED:
        assert f"def {name}" not in source, f"bar-era Discord builder remains: {name}"


def test_discord_keeps_webhook_plumbing() -> None:
    source = _read("src/utils/discord.py")
    for name in DISCORD_RETAINED:
        assert f"def {name}" in source, f"webhook plumbing was lost: {name}"


def test_discord_reaches_no_disk_database() -> None:
    source = _read("src/utils/discord.py")
    for needle in ("src.database", "duckdb"):
        assert needle not in source, f"disk-database reference remains in discord.py: {needle}"


def test_capital_credentials_are_retained() -> None:
    """RMV-07: live auth depends on them."""
    import src.credentials as credentials

    assert hasattr(credentials, "get_capital_credentials")


# ----------------------------------------------------------- RMV-04 dependencies

DROPPED_DEPENDENCIES = ("yfinance", "polygon-api-client")

RETAINED_DEPENDENCIES = (
    "duckdb",
    "pyarrow",
    "pandas",
    "pytz",
    "tzdata",
    "websockets",
    "requests",
    "python-dotenv",
    "psutil",
    "pytest",
    "databento",
)


def _requirement_names() -> set[str]:
    names = set()
    for line in _read("requirements.txt").splitlines():
        line = line.split("#", 1)[0].strip()
        if not line:
            continue
        names.add(line.split("==")[0].split(">=")[0].strip().lower())
    return names


def test_dead_provider_dependencies_are_removed() -> None:
    names = _requirement_names()
    for dependency in DROPPED_DEPENDENCIES:
        assert dependency not in names, f"dead dependency remains: {dependency}"


def test_retained_dependencies_survive() -> None:
    names = _requirement_names()
    missing = [d for d in RETAINED_DEPENDENCIES if d not in names]
    assert not missing, f"retained dependencies were removed: {missing}"


# ------------------------------------------------------------- RMV-07 config.py

DEAD_BAR_CONSTANTS = ("BINANCE_DOMAINS", "BAHRAIN_TZ")


def test_dead_bar_constants_are_removed_from_config() -> None:
    source = _read("src/config.py")
    for name in DEAD_BAR_CONSTANTS:
        assert name not in source, f"dead constant remains in src/config.py: {name}"


def test_config_keeps_the_approved_symbol_scope() -> None:
    from src.config import APPROVED_EQUITY_SYMBOLS

    assert len(APPROVED_EQUITY_SYMBOLS) == 19


# -------------------------------------------------------- RMV-02 bar pipeline

BAR_PIPELINE_FILES = (
    "main.py",
    "src/data/harvester.py",
    "src/data/normalizer.py",
    "src/api/massive.py",
    "src/api/yahoo.py",
    "src/api/binance.py",
    "tools/backfill_massive.py",
    "tools/benchmark_baseline.py",
    "tools/audit_database_integrity.py",
    "src/dashboard/harvester_job.py",
)

BAR_PIPELINE_MODULES = (
    "src.data.harvester",
    "src.data.normalizer",
    "src.api.massive",
    "src.api.yahoo",
    "src.api.binance",
    "src.dashboard.harvester_job",
)


def test_bar_pipeline_files_are_deleted() -> None:
    for relative in BAR_PIPELINE_FILES:
        assert not (REPO_ROOT / relative).exists(), f"bar-pipeline file survived: {relative}"


def test_no_module_imports_the_bar_pipeline() -> None:
    offenders = []
    for path in (REPO_ROOT / "src").rglob("*.py"):
        source = path.read_text(encoding="utf-8")
        for module in BAR_PIPELINE_MODULES:
            if f"import {module}" in source or f"from {module}" in source:
                offenders.append(f"{path.relative_to(REPO_ROOT)} -> {module}")
    assert not offenders, f"modules still import the bar pipeline: {offenders}"


# --------------------------------------------------- RMV-03 bar-era frontend

BAR_ERA_FRONTEND_TOKENS = (
    "/api/harvester",
    "harvester.js",
    "triggerHarvestRun",
    "openHarvestModal",
    "pollHarvesterLogs",
    "clearTerminalLogs",
    "source=historical",
    "db_source",
    "Massive",
    "Yahoo",
)


def test_frontend_has_no_bar_era_surfaces() -> None:
    offenders = []
    for path in (REPO_ROOT / "src" / "dashboard" / "static").rglob("*"):
        if path.suffix not in (".html", ".js"):
            continue
        source = path.read_text(encoding="utf-8")
        for token in BAR_ERA_FRONTEND_TOKENS:
            if token in source:
                offenders.append(f"{path.relative_to(REPO_ROOT)}: {token}")
    assert not offenders, f"bar-era frontend surfaces remain: {offenders}"


def test_harvester_script_is_gone_and_symbols_script_is_served() -> None:
    js_dir = REPO_ROOT / "src" / "dashboard" / "static" / "js"
    assert not (js_dir / "harvester.js").exists()
    assert (js_dir / "symbols.js").exists()
    assert "/static/js/symbols.js" in _read("src/dashboard/static/index.html")


def test_server_exposes_no_harvester_routes() -> None:
    source = _read("src/dashboard/server.py")
    assert "/api/harvester" not in source
    assert "harvester_manager" not in source


# ------------------------------------------------- RMV-10 crypto stream residue

CRYPTO_STREAM_FILES = (
    "src/stream/binance_stream.py",
    "src/stream/aggregator.py",
)

CRYPTO_STREAM_TOKENS = (
    "BinanceStreamer",
    "BinanceKline",
    "CandleAggregator",
    "binance_streamer",
    "binance_ticker",
    "enable_binance",
    "_handle_binance_tick",
    "_handle_binance_bar",
    "--enable-binance",
)


def test_crypto_stream_files_are_deleted() -> None:
    for relative in CRYPTO_STREAM_FILES:
        assert not (REPO_ROOT / relative).exists(), f"crypto stream file survived: {relative}"


def test_no_crypto_stream_api_survives_in_src() -> None:
    offenders = []
    for path in (REPO_ROOT / "src").rglob("*.py"):
        source = path.read_text(encoding="utf-8")
        for token in CRYPTO_STREAM_TOKENS:
            if token in source:
                offenders.append(f"{path.relative_to(REPO_ROOT)}: {token}")
    assert not offenders, f"crypto stream API survived: {offenders}"


def test_streaming_scope_is_the_equities_only() -> None:
    """The runner takes its symbols from the equity registry; crypto is not an option."""
    source = _read("src/stream/runner.py")
    for token in ("BTC", "USDT", "wss://stream.binance.com"):
        assert token not in source, f"crypto residue in the runner: {token}"
