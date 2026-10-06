"""Phase 52: the three public documents describe the quote columns and the gap fill."""

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DOCUMENTS = (
    PROJECT_ROOT / "README.md",
    PROJECT_ROOT / "docs" / "operations" / "tick_lake_operations_guide.md",
    PROJECT_ROOT / "docs" / "contracts" / "repo_b_tick_lake_contract.md",
)

STORED_QUOTE = """New rows store `bid_price` and `ask_price` only, plus `timestamp`, `symbol`, `source`, `session`, and `ingest_id`. Do not read `price` or `volume` as the stored quote. Capital.com stores its quote-change bid and ask. Databento `tbbo` stores `bid_px_00` as `bid_price` and `ask_px_00` as `ask_price`. Fewer Databento rows than Capital.com rows is accepted.

The existing lake is still schema v1. Those files still contain `price`, `volume`, `bid`, and `ask`. The quote in a schema v1 file is `bid` and `ask`, not `price` or `volume`. The rewrite that copies `bid` to `bid_price` and `ask` to `ask_price` is available as `python -m src.storage.quote_rewrite --lake-root <explicit-lake> --backup-root <explicit-backup>`. Production execution is deferred to the owner. Do not run that rewrite against the production lake from this checkout.

Gap fill names one day and requests Databento `tbbo` only for stretches inside 04:00-20:00 ET where every active registry symbol is silent. A minute where one symbol has a tick is not requested. Pre-market (04:00-09:30 ET) and post-market (16:00-20:00 ET) count at 15 minutes or more. Regular hours (09:30-16:00 ET) count at 2 minutes or more. A stretch that crosses 09:30 or 16:00 is split, and each piece uses its own rule. Weekends, full NYSE holidays, and the time after an official early close (13:00 ET) are not requested. Gap fill does not start while the live writer holds the publisher lock. It appends new rows and does not rewrite existing files. Name exactly one day with `python -m src.data.gap_fill --date YYYY-MM-DD`."""


def test_each_public_document_states_the_quote_and_gap_fill_rule():
    """README, the operations guide, and the Repo B contract carry the same rule."""
    for path in DOCUMENTS:
        text = path.read_text(encoding="utf-8")
        assert STORED_QUOTE in text, f"{path.name} does not state the quote and gap-fill rule"
        assert "Observed quote/trade price" not in text, path.name
        assert "Run the rewrite" not in text, path.name
        assert "is not available from this checkout" not in text, path.name
        assert "python -m src.storage.quote_rewrite --lake-root <explicit-lake> --backup-root <explicit-backup>" in text, path.name
