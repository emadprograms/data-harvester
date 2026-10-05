"""
Documentation matches the code (Phase 36 / DOCS-01, DOCS-02, DOCS-03).

Documentation rots silently, so these tests read the published documents and
compare them against the code they describe: schema tables against
`src/storage/schema.py`, runtime defaults against constructor signatures, the
lake-root precedence chain against the resolver, and the fail-closed backend
rule against the reader-selection function.

Findings pinned here:

- **F12** — the operations guide documented the runner's flush interval as
  `2.0s`; `DEFAULT_STREAM_FLUSH_INTERVAL_SECONDS` is `5.0`. Corrected.
- **F13** — archived v4.0 text described an "8-column schema" while also naming
  `ingest_id`, which is self-contradictory; there are nine columns. Corrected.
"""

from __future__ import annotations

import ast
import inspect
import re
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from src.dashboard import analytics
from src.storage.config import (
    DATA_DIR_ENV,
    DEFAULT_LAKE_SUBDIR,
    MICRON_DATA_DIR,
    TICK_LAKE_ROOT_ENV,
    resolve_tick_lake_root,
)
from src.storage.parquet_writer import TickLakeWriter
from src.storage.reader import TickLakeReader
from src.storage.schema import LAKE_SCHEMA_V1, SCHEMA_V1_COLUMNS
from src.stream.runner import (
    DEFAULT_STREAM_FLUSH_INTERVAL_SECONDS,
    DEFAULT_STREAM_MAX_BATCH_ROWS,
    StreamingEngine,
)
from tests.support.contract_examples import CONTRACT_DOC, load_contract_namespace
from tests.support.lake_assertions import finalized_parquet_files

PROJECT_ROOT = Path(__file__).resolve().parents[2]
OPS_GUIDE = PROJECT_ROOT / "docs" / "operations" / "tick_lake_operations_guide.md"
README = PROJECT_ROOT / "README.md"
DOCS_DIR = PROJECT_ROOT / "docs"


# ------------------------------------------------------------------ helpers


def _markdown_table_rows(text: str, header_marker: str) -> list:
    """Rows of the first markdown table whose header contains `header_marker`."""
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if line.startswith("|") and header_marker in line:
            rows = []
            cursor = index + 2  # skip the separator row
            while cursor < len(lines) and lines[cursor].startswith("|"):
                rows.append([c.strip() for c in lines[cursor].strip("|").split("|")])
                cursor += 1
            return rows
    raise AssertionError(f"no markdown table found with header {header_marker!r}")


def _fenced_python_blocks(path: Path) -> list:
    text = path.read_text(encoding="utf-8")
    return re.findall(r"```python\n(.*?)```", text, re.DOTALL)


# ------------------------------------------------------------------ DOCS-01


def test_documented_schema_columns_match_the_code_schema():
    """The nine documented columns, in order, with the documented nullability."""
    rows = _markdown_table_rows(OPS_GUIDE.read_text(encoding="utf-8"), "| Column |")
    documented = [row[0].strip("`") for row in rows]

    assert documented == list(SCHEMA_V1_COLUMNS), (
        f"documented columns {documented} do not match the code schema {list(SCHEMA_V1_COLUMNS)}"
    )
    assert len(documented) == 9, f"expected nine columns, documented {len(documented)}"

    expected_nullable = {f.name: f.nullable for f in LAKE_SCHEMA_V1}
    for row in rows:
        column = row[0].strip("`")
        documented_nullable = row[2].strip().lower() == "yes"
        assert documented_nullable == expected_nullable[column], (
            f"{column}: documented nullable={documented_nullable}, code says {expected_nullable[column]}"
        )


def test_documented_schema_types_match_a_generated_lake(contract_lake):
    """Documented logical types agree with the physical Parquet types."""
    rows = _markdown_table_rows(OPS_GUIDE.read_text(encoding="utf-8"), "| Column |")
    # Type cells look like "`TIMESTAMP` (naive UTC, us)": keep only the type token.
    documented = {row[0].strip("`"): row[1].split("(")[0].strip().strip("`") for row in rows}

    files = finalized_parquet_files(contract_lake)
    assert files, "no finalized files to compare against"
    physical = {f.name: str(f.type) for f in pq.ParquetFile(files[0]).schema_arrow}

    # Classify from the code schema rather than from the column name.
    for field in LAKE_SCHEMA_V1:
        column = field.name
        declared = documented[column].upper()
        if str(field.type) == "timestamp[us]":
            assert declared == "TIMESTAMP", f"{column}: documented {declared}"
            assert "timestamp" in physical[column], f"{column}: physical {physical[column]}"
        elif str(field.type) == "double":
            assert declared == "DOUBLE", f"{column}: documented {declared}"
            assert physical[column] == "double", f"{column}: physical {physical[column]}"
        elif str(field.type) == "string":
            assert declared == "VARCHAR", f"{column}: documented {declared}"
            # Physically dictionary-encoded: still a string logically.
            assert "string" in physical[column], f"{column}: physical {physical[column]}"
        else:  # pragma: no cover - guards against schema additions
            raise AssertionError(f"unhandled schema type for {column}: {field.type}")


def test_repo_b_contract_schema_section_agrees_with_the_code():
    """The Repo B contract's schema table matches the same code schema."""
    text = CONTRACT_DOC.read_text(encoding="utf-8")
    rows = _markdown_table_rows(text, "Column Name")
    documented = [row[0].strip("`") for row in rows]
    assert documented == list(SCHEMA_V1_COLUMNS), (
        f"contract columns {documented} do not match {list(SCHEMA_V1_COLUMNS)}"
    )


def test_no_document_claims_an_eight_column_schema():
    """F13: the schema has nine columns; no document may say otherwise."""
    offenders = []
    for path in list(DOCS_DIR.rglob("*.md")) + list((PROJECT_ROOT / ".planning").rglob("*.md")) + [README]:
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if re.search(r"\b8[- ]column\b|\beight columns?\b", line, re.I):
                # The correction note names the old error deliberately.
                if "Corrected in v4.2" in line:
                    continue
                # Eight is correct for the legacy streaming.duckdb schema, which
                # has no ingest_id. Only the tick lake has nine.
                if re.search(r"legacy|streaming\.duckdb|src/database/schema\.py|init_streaming_db", line, re.I):
                    continue
                # The signoff record of this discrepancy is itself documentation.
                if "Observed discrepancies" in line or "Discrepancies observed" in line:
                    continue
                # Findings tables quote the defect they describe on purpose.
                if line.lstrip().startswith("| F") or line.lstrip().startswith("| D"):
                    continue
                # A line that names the correct count alongside the wrong one is
                # describing the defect, not asserting it.
                if re.search(r"nine|correct(ed|ion)", line, re.I):
                    continue
                offenders.append(f"{path.relative_to(PROJECT_ROOT)}:{number}: {line.strip()[:100]}")
    assert offenders == [], f"documents still describe an eight-column schema:\n" + "\n".join(offenders)


def test_every_python_example_in_the_documentation_parses():
    """Public examples must at least be valid Python; rot is caught here."""
    checked = 0
    for path in list(DOCS_DIR.rglob("*.md")) + [README]:
        for block in _fenced_python_blocks(path):
            try:
                ast.parse(block)
            except SyntaxError as exc:
                pytest.fail(f"{path.relative_to(PROJECT_ROOT)}: unparseable python example: {exc}")
            checked += 1
    assert checked, "no python examples found in the documentation"


def test_documented_repo_b_examples_execute(contract_lake):
    """DOCS-01: public examples execute successfully against generated fixtures."""
    namespace = load_contract_namespace()
    reader = namespace["RepoBTickReader"](str(contract_lake))
    candles = reader.query_candles(
        "AAPL",
        __import__("datetime").date(2026, 10, 1),
        __import__("datetime").date(2026, 11, 30),
        "1m",
    )
    assert candles, "the documented reader example returned no candles"


# ------------------------------------------------------------------ DOCS-02


def test_documented_flush_defaults_match_the_code():
    """F12: the runner's flush default is 5.0s, not the documented 2.0s."""
    rows = _markdown_table_rows(OPS_GUIDE.read_text(encoding="utf-8"), "| Setting |")
    documented = {row[0]: row[2] for row in rows}

    flush_default = inspect.signature(StreamingEngine.__init__).parameters["flush_interval"].default
    assert flush_default is None, (
        "the runner now accepts a concrete flush default; update the document and this assertion"
    )
    # Every number in the cell must agree with the runner default. A cell that
    # read "`2.0s` in the runner; `5.0s` writer-class default" would satisfy a
    # plain substring check while still documenting the wrong runner value.
    documented_flush = {
        float(value)
        for value in re.findall(r"([0-9]+(?:\.[0-9]+)?)s", documented["Flush interval"])
    }
    assert documented_flush, f"no flush interval documented: {documented['Flush interval']!r}"
    assert documented_flush == {DEFAULT_STREAM_FLUSH_INTERVAL_SECONDS}, (
        f"documented flush interval(s) {documented_flush} do not match "
        f"DEFAULT_STREAM_FLUSH_INTERVAL_SECONDS={DEFAULT_STREAM_FLUSH_INTERVAL_SECONDS}"
    )
    assert str(DEFAULT_STREAM_MAX_BATCH_ROWS) in documented["Max rows per batch"], (
        f"documented batch size {documented['Max rows per batch']!r} does not match "
        f"{DEFAULT_STREAM_MAX_BATCH_ROWS}"
    )


def test_documented_writer_and_reader_defaults_match_signatures():
    """The documented defaults equal the actual constructor defaults."""
    rows = _markdown_table_rows(OPS_GUIDE.read_text(encoding="utf-8"), "| Setting |")
    documented = {row[0]: row[2] for row in rows}

    writer_defaults = inspect.signature(TickLakeWriter.__init__).parameters
    assert str(writer_defaults["max_batch_rows"].default) in documented["Max rows per batch"]
    assert writer_defaults["compression"].default in documented["Compression codec"]
    assert str(writer_defaults["retry_attempts"].default) in documented["Writer retry policy"]

    reader_defaults = inspect.signature(TickLakeReader.__init__).parameters
    assert str(reader_defaults["max_threads"].default) in documented["Reader threads / memory"]
    assert reader_defaults["memory_limit"].default in documented["Reader threads / memory"]


def test_documented_lake_root_precedence_matches_the_resolver():
    """The documented chain names every rule, in order, with the real paths."""
    text = OPS_GUIDE.read_text(encoding="utf-8")
    match = re.search(r"\*\*Lake-root resolution precedence\*\*.*?`\.\s", text, re.S)
    assert match, "the operations guide does not document lake-root precedence"
    chain = match.group(0)

    assert "TICK_LAKE_ROOT" in chain and "DATA_DIR" in chain, chain
    assert MICRON_DATA_DIR in chain, (
        f"documented micron path does not match MICRON_DATA_DIR={MICRON_DATA_DIR!r}"
    )
    assert chain.index("TICK_LAKE_ROOT") < chain.index("DATA_DIR"), (
        "documented precedence puts DATA_DIR before TICK_LAKE_ROOT"
    )
    assert chain.index("DATA_DIR") < chain.index("Micron"), (
        "documented precedence puts the micron path before DATA_DIR"
    )
    assert "StorageConfigError" in chain, "the documented chain does not end in StorageConfigError"


def test_resolution_precedence_behaves_as_documented(tmp_path, monkeypatch):
    """Exercise the documented chain: explicit argument beats TICK_LAKE_ROOT."""
    env_root = tmp_path / "from_env"
    data_root = tmp_path / "from_data_dir"
    explicit = tmp_path / "explicit"
    for path in (env_root, data_root, explicit):
        path.mkdir()

    monkeypatch.setenv(TICK_LAKE_ROOT_ENV, str(env_root))
    monkeypatch.setenv(DATA_DIR_ENV, str(data_root))
    monkeypatch.delenv("MICRON_DATA_DIR", raising=False)

    assert resolve_tick_lake_root() == env_root.resolve()
    assert resolve_tick_lake_root(str(explicit)) == explicit.resolve()

    monkeypatch.delenv(TICK_LAKE_ROOT_ENV)
    resolved = resolve_tick_lake_root()
    assert resolved == (data_root / DEFAULT_LAKE_SUBDIR).resolve(), (
        f"without TICK_LAKE_ROOT the lake must resolve under DATA_DIR, got {resolved}"
    )


def test_backend_selection_fails_closed_when_the_lake_is_explicit(monkeypatch):
    """DOCS-03: an explicitly selected lake raises instead of opening legacy."""
    missing_root = "/nonexistent/lake/that/does/not/exist"
    monkeypatch.setenv(TICK_LAKE_ROOT_ENV, missing_root)
    monkeypatch.delenv(DATA_DIR_ENV, raising=False)

    # v5.0 removed the disk-database backend outright: there is no legacy
    # connection helper left for the lake to fall back to.
    assert not hasattr(analytics, "get_streaming_db_connection")

    with pytest.raises(Exception):
        analytics.get_stream_tape(symbol="AAPL", limit=5)


def test_autodetection_still_uses_the_lake_when_it_is_populated(contract_lake, docs_data_dir, monkeypatch):
    """With no explicit selection, a populated lake is chosen over legacy."""
    monkeypatch.delenv(TICK_LAKE_ROOT_ENV, raising=False)
    # No explicit selection: the lake must be autodetected under DATA_DIR.
    monkeypatch.setenv(DATA_DIR_ENV, str(docs_data_dir))

    assert not hasattr(analytics, "get_streaming_db_connection")

    result = analytics.get_stream_tape(symbol="AAPL", limit=5)
    assert result is not None




# ------------------------------------------------------------------ DOCS-04

AUDIT_REPORT = PROJECT_ROOT / "docs" / "plans" / "milestone-4.2-audit-report.md"


def test_audit_report_exists_and_publishes_every_required_element():
    """DOCS-04: the published report carries each element the requirement names."""
    assert AUDIT_REPORT.is_file(), f"missing audit report at {AUDIT_REPORT}"
    text = AUDIT_REPORT.read_text(encoding="utf-8")

    required = {
        "requirement matrix": "Requirement matrix",
        "CI evidence": "Test evidence",
        "benchmark artifacts": "Benchmark artifacts",
        "migration/restore reports": "Migration, backup & restore",
        "Repo B result": "Repo B read contract",
        "durability contract": "Durability",
        "explicit deferred scope": "Explicitly out of scope",
    }
    missing = [
        name for name, marker in required.items() if marker not in text
    ]
    assert missing == [], f"the audit report is missing: {missing}"


def test_audit_report_deferred_scope_names_every_deferred_requirement():
    """DOCS-04: Q10 replay and compaction are visibly excluded, not omitted."""
    text = AUDIT_REPORT.read_text(encoding="utf-8")
    for requirement in ("RPLY-01", "COMP-01", "ENDR-01", "CAPA-01", "DURB-04"):
        assert requirement in text, (
            f"deferred requirement {requirement} is not named in the audit report"
        )
    assert "not covered by any statement in this report" in text


def test_audit_report_cites_real_artifacts():
    """DOCS-04: every artifact the report cites must actually exist."""
    text = AUDIT_REPORT.read_text(encoding="utf-8")
    cited = [
        line
        for line in text.splitlines()
        if line.strip().startswith("|") and "`" in line and ".json" in line or ".md`" in line
    ]
    checked = 0
    for line in cited:
        for fragment in re.findall(r"`([^`]+)`", line):
            if not fragment.endswith((".md", ".json", ".py")):
                continue
            path = PROJECT_ROOT / fragment
            if not path.exists():
                # Braced families such as lake-scale-write-{...}.json are patterns.
                if "{" in fragment:
                    continue
                pytest.fail(f"audit report cites a missing artifact: {fragment}")
            checked += 1
    assert checked, "no artifact paths were verified in the audit report"


@pytest.fixture(scope="module")
def docs_data_dir(tmp_path_factory):
    """`DATA_DIR` whose `tick_lake` subdirectory is the lake under test."""
    return tmp_path_factory.mktemp("docs_contract")


@pytest.fixture(scope="module")
def contract_lake(docs_data_dir):
    """A small lake with AAPL rows, built by the product writer."""
    from datetime import datetime

    from src.storage.config import init_tick_lake
    from src.storage.parquet_writer import TickLakeWriter
    from tests.fixtures.deterministic_quotes import QuoteTick

    root = docs_data_dir / DEFAULT_LAKE_SUBDIR
    init_tick_lake(root)
    writer = TickLakeWriter(root=root, writer_id="docs", max_batch_rows=10**9)
    writer.write_ticks(
        [
            QuoteTick(
                timestamp=datetime(2026, 10, 2, 14, 30, index),
                symbol="AAPL",
                price=150.0 + index,
                volume=1.0,
                source="CAPITAL",
                session="REG",
                ingest_id=f"docs_{index:03d}",
            )
            for index in range(10)
        ]
    )
    writer.flush(block=True)
    writer.close()
    return root
