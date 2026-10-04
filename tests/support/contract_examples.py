"""
Execute the documented Repo B contract examples (Phase 33 / REPB-01, REPB-02).

The examples in `docs/contracts/repo_b_tick_lake_contract.md` are extracted from
the markdown itself and then executed. This is deliberate: the documentation is
the artifact Repo B copies, so if an example is edited, these tests run the
edited version. A near-copy of the example kept in the test tree would let the
two drift silently and would prove nothing about what Repo B actually pastes.

`run_isolated` additionally executes the examples in a **separate process with
`src` imports blocked by an import hook**, so "zero data-harvester imports" is
demonstrated rather than asserted in a comment.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONTRACT_DOC = PROJECT_ROOT / "docs" / "contracts" / "repo_b_tick_lake_contract.md"

_BLOCK_RE = re.compile(r"```python\n(.*?)```", re.DOTALL)

# Installed before the contract examples: any attempt to import the
# data-harvester package raises, so a hidden coupling fails loudly.
_ISOLATED_PRELUDE = """
import json
import sys


class _SrcImportBlocker:
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "src" or fullname.startswith("src."):
            raise ImportError(
                "contract example attempted to import data-harvester source: " + fullname
            )
        return None


sys.meta_path.insert(0, _SrcImportBlocker())
"""


def extract_python_blocks(doc_path: Path = CONTRACT_DOC) -> List[str]:
    """Return every ```python block in the contract document, in document order."""
    text = Path(doc_path).read_text(encoding="utf-8")
    blocks = [match.group(1) for match in _BLOCK_RE.finditer(text)]
    if not blocks:
        raise AssertionError(f"no python examples found in {doc_path}")
    return blocks


def contract_source(doc_path: Path = CONTRACT_DOC) -> str:
    """All python examples concatenated, in order, as one importable module body.

    Later blocks rely on imports made in the first block (duckdb, Path, date),
    so they must be executed together rather than independently.
    """
    return "\n\n".join(extract_python_blocks(doc_path))


def load_contract_namespace(doc_path: Path = CONTRACT_DOC) -> Dict[str, Any]:
    """Execute the documented examples in-process and return their namespace."""
    namespace: Dict[str, Any] = {}
    exec(compile(contract_source(doc_path), str(doc_path), "exec"), namespace)
    return namespace


def assert_no_product_imports(doc_path: Path = CONTRACT_DOC) -> None:
    """Static check: no example may reference the data-harvester package."""
    for index, block in enumerate(extract_python_blocks(doc_path), start=1):
        for line in block.splitlines():
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            assert "src." not in stripped and "from src" not in stripped, (
                f"python block {index} imports the data-harvester package: {stripped!r}"
            )


def run_isolated(
    snippet: str,
    *,
    payload: Any = None,
    timeout: float = 300.0,
    python: str = sys.executable,
) -> Any:
    """Run `snippet` after the contract examples, in a subprocess with `src` blocked.

    The snippet receives the JSON `payload` on stdin and must print a single JSON
    value on stdout. Its namespace contains everything the examples defined,
    including `RepoBTickReader`, `query_tape`, `scan_ticks_with_arrow`,
    `is_lake_maintenance_in_progress`, `duckdb`, `ds` and `pc`.
    """
    script = _ISOLATED_PRELUDE + "\n" + contract_source() + "\n\n" + snippet

    # Drop PYTHONPATH and run from a neutral cwd so the repository root is not
    # importable; the meta_path blocker above is the belt to this braces.
    env = {key: value for key, value in os.environ.items() if key != "PYTHONPATH"}
    with tempfile.TemporaryDirectory() as neutral_cwd:
        completed = subprocess.run(
            [python, "-c", script],
            input=json.dumps(payload),
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=neutral_cwd,
            env=env,
        )

    if completed.returncode != 0:
        raise AssertionError(
            "isolated contract example failed with exit code "
            f"{completed.returncode}\n--- stdout ---\n{completed.stdout}\n"
            f"--- stderr ---\n{completed.stderr}"
        )

    stdout = completed.stdout.strip()
    if not stdout:
        raise AssertionError(
            f"isolated contract example produced no JSON output\n--- stderr ---\n{completed.stderr}"
        )
    return json.loads(stdout)
