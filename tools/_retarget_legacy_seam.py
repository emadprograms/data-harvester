"""Retarget a legacy streaming test file from the in-memory DuckDB seam to the lake.

Run with:  /tmp/dhvenv/bin/python tools/_retarget_legacy_seam.py <file> [--symbols A,B]
Not part of the shipped tooling: a one-shot migration helper for Phase 47.
"""
from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

LAKE_IMPORT = "from tests.support.lake_population import create_lake, publish_minutes\n"

LAKE_SETUP_TEMPLATE = """{indent}lake = create_lake(tmp_path / "lake", symbols={symbols})
{indent}monkeypatch.setenv("TICK_LAKE_ROOT", str(lake))
{indent}monkeypatch.setenv("DATA_DIR", str(lake))
"""


def _drop_function(text: str, name: str) -> str:
    """Removes a top-level or nested function definition by name."""
    pattern = re.compile(rf"^(\s*)def {re.escape(name)}\(", re.M)
    match = pattern.search(text)
    if not match:
        return text
    indent = match.group(1)
    body = re.compile(rf"^{re.escape(indent)}(def |class |@)", re.M)
    nxt = body.search(text, match.end())
    end = nxt.start() if nxt else len(text)
    while end > 0 and text[end - 1] == "\n" and text[end - 2 : end] == "\n\n":
        end -= 1
    return text[: match.start()] + text[end:]


def retarget(path: Path, symbols: list[str]) -> None:
    text = path.read_text(encoding="utf-8")
    original = text
    symbol_literal = "[" + ", ".join(f'"{s}"' for s in symbols) + "]"

    # 1. imports
    text = _strip_database_imports(text)
    if "from tests.support.lake_population" not in text:
        text = _insert_import(text, LAKE_IMPORT)

    # 2. local scaffolding helpers
    text = _drop_function(text, "create_in_memory_streaming_db")
    for helper in ("populate_ticks_for_minutes", "populate_ticks_at_et",
                   "populate_trading_session_ticks", "populate_extended_session_ticks",
                   "populate_ticks_for_day"):
        text = _drop_function(text, helper)

    # 3. per-test setup
    text = re.sub(
        r"^(\s*)client = create_in_memory_streaming_db\(\)\n",
        lambda m: LAKE_SETUP_TEMPLATE.format(indent=m.group(1), symbols=symbol_literal),
        text,
        flags=re.M,
    )
    text = re.sub(
        r"^(\s*)mem_client = create_in_memory_streaming_db\(\)\n",
        lambda m: LAKE_SETUP_TEMPLATE.format(indent=m.group(1), symbols=symbol_literal),
        text,
        flags=re.M,
    )

    # 4. publication calls
    for helper in ("populate_ticks_for_minutes", "populate_ticks_at_et",
                   "populate_trading_session_ticks", "populate_extended_session_ticks",
                   "populate_ticks_for_day"):
        text = re.sub(rf"\b{helper}\(\s*(?:mem_client|client)\s*,\s*", "publish_minutes(lake, ", text)

    # 5. drop the injected client
    text = re.sub(r"client=(?:mem_client|client),\s*", "", text)
    text = re.sub(r",\s*client=(?:mem_client|client)\b", "", text)
    text = re.sub(r"client=(?:mem_client|client)\b", "", text)
    text = re.sub(r"^\s*, client=client\n", "", text, flags=re.M)

    # 6. fixtures for tests that now need tmp_path/monkeypatch
    text = _add_fixtures(text)

    # 7. stale references
    for stale in ("DuckDBClient", "init_streaming_db", "init_historical_db",
                  "create_in_memory_streaming_db", "mem_client", "client=client",
                  "get_streaming_db_connection", "get_historical_db_connection"):
        if stale in text:
            print(f"  ! still present: {stale}")

    ast.parse(text)
    path.write_text(text, encoding="utf-8")
    print(f"  changed: {text != original}")


def _strip_database_imports(text: str) -> str:
    """Removes any import of src.database, including parenthesized multi-line forms."""
    tree = ast.parse(text)
    lines = text.split("\n")
    doomed: set[int] = set()
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and (node.module or "").startswith("src.database"):
            for lineno in range(node.lineno, (node.end_lineno or node.lineno) + 1):
                doomed.add(lineno)
    kept = [line for index, line in enumerate(lines, start=1) if index not in doomed]

    # Names imported from elsewhere but belonging to the deleted layer.
    text = "\n".join(kept)
    for name in ("DuckDBClient", "init_streaming_db", "init_historical_db",
                 "get_streaming_db_connection", "get_historical_db_connection",
                 "get_duckdb_connection", "init_db"):
        text = re.sub(rf"^(\s*){re.escape(name)},\n", "", text, flags=re.M)
    return text


def _insert_import(text: str, statement: str) -> str:
    """Inserts a module-level import after the last existing top-level import."""
    tree = ast.parse(text)
    last = 0
    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)) and node.end_lineno:
            last = max(last, node.end_lineno)
    lines = text.split("\n")
    if last == 0:
        return statement + text
    lines.insert(last, statement.rstrip("\n"))
    return "\n".join(lines)


def _add_fixtures(text: str) -> str:
    """Adds tmp_path/monkeypatch to any test whose body now builds a lake."""
    lines = text.split("\n")
    targets: list[tuple[int, str, str, bool]] = []
    for index, line in enumerate(lines):
        match = re.match(r"^(\s*)def (test_\w+)\((?P<params>[^)]*)\):", line)
        if not match:
            continue
        indent, name = match.group(1), match.group(2)
        params = match.group("params")
        body_end = index
        for j in range(index + 1, len(lines)):
            if re.match(rf"^{indent}(def |class |@)", lines[j]):
                break
            body_end = j
        body = "\n".join(lines[index + 1 : body_end + 1])
        if "create_lake(" not in body:
            continue
        additions = [p for p in ("tmp_path", "monkeypatch") if p not in params]
        if not additions:
            continue
        targets.append((index, indent, name, params))

    for index, indent, name, params in reversed(targets):
        additions = [p for p in ("tmp_path", "monkeypatch") if p not in params]
        existing = [p.strip() for p in params.split(",") if p.strip()]
        new_params = ", ".join(existing + additions)
        lines[index] = f"{indent}def {name}({new_params}):"
    return "\n".join(lines)


if __name__ == "__main__":
    target = Path(sys.argv[1])
    syms = ["NVDA", "AAPL", "MSFT", "SPY", "TEST_SYM", "BOUNDARY_SYM", "CONSISTENCY_SYM", "ADBE", "AMD", "APP", "TSLA"]
    if "--symbols" in sys.argv:
        syms = sys.argv[sys.argv.index("--symbols") + 1].split(",")
    print(f"retargeting {target} symbols={syms}")
    retarget(target, syms)
