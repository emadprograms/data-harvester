"""Provenance metadata in the v6.0 milestone audit must describe something real.

WR-01: the audit kept one `candidate_sha` for work that landed across several
revisions, and a note claimed that candidate was absent from the checkout even
though it is an ancestor of the merge the audit signed off. These tests pin the
two properties that were violated:

1. a revision the document calls absent must actually be absent, and
2. findings from a re-audit must be recorded with the revision that closed them.
"""

import re
import subprocess
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
AUDIT_PATH = PROJECT_ROOT / ".planning" / "milestones" / "v6.0-MILESTONE-AUDIT.md"

ABSENCE_CLAIM = re.compile(
    r"not present|absent|does not exist|missing from this checkout|cannot be resolved",
    re.IGNORECASE,
)
SHA = re.compile(r"\b([0-9a-f]{7,40})\b")
FULL_SHA = re.compile(r"^[0-9a-f]{40}$")
REAUDIT_FINDINGS = ("CR-01", "CR-02", "CR-03", "CR-04")


def _frontmatter() -> str:
    match = re.match(r"^---\n(.*?)\n---\n", AUDIT_PATH.read_text(encoding="utf-8"), re.DOTALL)
    assert match, f"{AUDIT_PATH.name} must open with a frontmatter block"
    return match.group(1)


def _block(frontmatter: str, key: str) -> str:
    """The indented lines that belong to one top-level frontmatter key."""
    collected: list[str] = []
    capturing = False
    for line in frontmatter.splitlines():
        if re.match(rf"^{re.escape(key)}:", line):
            capturing = True
            collected.append(line)
            continue
        if capturing:
            if line and not line.startswith((" ", "\t")):
                break
            collected.append(line)
    return "\n".join(collected)


def _commit_exists(sha: str) -> bool:
    """True when this checkout can resolve the revision as a commit."""
    result = subprocess.run(
        ["git", "cat-file", "-e", f"{sha}^{{commit}}"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def _is_shallow_clone() -> bool:
    """True when history is truncated, so ancestor revisions are simply not present."""
    result = subprocess.run(
        ["git", "rev-parse", "--is-shallow-repository"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    return result.returncode == 0 and result.stdout.strip() == "true"


def test_audit_records_a_full_candidate_and_its_history():
    frontmatter = _frontmatter()
    candidate = re.search(r"^candidate_sha:\s*(\S+)\s*$", frontmatter, re.MULTILINE)
    assert candidate, "the audit must record candidate_sha"
    assert FULL_SHA.match(candidate.group(1)), (
        f"candidate_sha must be a full 40-hex revision, got {candidate.group(1)!r}"
    )
    # Historical revisions stay recorded rather than being replaced by the latest one.
    assert "remediation:" in frontmatter
    assert re.search(r"^\s+sha:\s*[0-9a-f]{40}\s*$", _block(frontmatter, "remediation"), re.MULTILINE), (
        "the remediation revision must be recorded as a full SHA"
    )


def test_audit_only_calls_a_revision_absent_when_it_really_is():
    """An absence claim about a revision this checkout can resolve is a false claim."""
    if _is_shallow_clone():
        # Every ancestor looks absent in a truncated history, so this check would pass
        # vacuously. Skip rather than imply the claim was verified.
        pytest.skip(
            "shallow clone: history is truncated, so an absence claim about an ancestor "
            "cannot be disproved. Check the repository out with full history."
        )
    offenders = []
    for line in AUDIT_PATH.read_text(encoding="utf-8").splitlines():
        if not ABSENCE_CLAIM.search(line):
            continue
        for sha in SHA.findall(line):
            if _commit_exists(sha):
                offenders.append((sha, line.strip()))
    assert not offenders, (
        "the audit claims these revisions are absent, but this checkout resolves them: "
        f"{offenders}"
    )


def test_reaudit_findings_are_recorded_with_the_revision_that_closed_them():
    block = _block(_frontmatter(), "reaudit")
    assert block, "the audit must carry a `reaudit` frontmatter block"
    for finding in REAUDIT_FINDINGS:
        assert finding in block, f"re-audit finding {finding} is not recorded"
    fix_sha = re.search(r"^\s+fix_sha:\s*(\S+)\s*$", block, re.MULTILINE)
    assert fix_sha, "each re-audit closure must name the revision that fixed it"
    assert re.fullmatch(r"[0-9a-f]{7,40}", fix_sha.group(1)), (
        f"fix_sha must be a commit revision, got {fix_sha.group(1)!r}"
    )
    if _is_shallow_clone():
        # A truncated history cannot resolve any ancestor, so resolvability is not
        # decidable here. Skip loudly rather than passing as if it were verified; CI
        # checks out full history so this branch is not the normal path.
        pytest.skip(
            "shallow clone: history is truncated, so a recorded revision cannot be "
            "resolved. Check the repository out with full history to verify provenance."
        )
    if _commit_exists(fix_sha.group(1)):
        return
    if _commit_exists(fix_sha.group(1)[:7]):
        return
    pytest.fail(
        f"reaudit.fix_sha {fix_sha.group(1)!r} does not resolve in this checkout, so the "
        "verification it claims cannot be reproduced"
    )


def test_reaudit_closure_names_the_tracked_regression_file():
    block = _block(_frontmatter(), "reaudit")
    tests = re.search(r"^\s+tests:\s*(\S+)\s*$", block, re.MULTILINE)
    assert tests, "the re-audit closure must name the tests that pin it"
    assert (PROJECT_ROOT / tests.group(1)).is_file(), (
        f"the named regression file {tests.group(1)} does not exist"
    )
