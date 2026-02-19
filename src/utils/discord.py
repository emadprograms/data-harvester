import requests
import os
import pandas as pd
import time


def send_discord_harvest_report(report_df: pd.DataFrame, target_date, total_rows):
    """
    Sends a compact ticker table to Discord, split across messages if needed.
    Failed/partial tickers are marked with ⚠️.
    """
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        return False

    try:
        header = f"🚜 **Harvest Complete** | `{target_date}`\n"

        if report_df.empty:
            _post(webhook_url, header + "No data harvested.")
            return True

        # Build compact table lines: "TICKER  | 751"  or "⚠️ TICKER  |   0"
        lines = []
        for _, row in report_df.sort_values("Ticker").iterrows():
            ticker = row["Ticker"]
            total = row["Total"]
            status = row.get("Status", "")
            is_bad = total == 0 or "❌" in status or "⚠️" in status
            marker = "⚠️" if is_bad else "  "
            lines.append(f"{marker} {ticker:<10} {total:>5}")

        footer = f"{'─'*20}\n   {'TOTAL':<10} {total_rows:>5}"

        # Split into chunks that fit within 2000 chars
        # Header + code block overhead ≈ 120 chars, leave room
        MAX_CONTENT = 1850
        chunks = []
        current_lines = []
        current_len = 0

        for line in lines:
            line_len = len(line) + 1  # +1 for newline
            if current_len + line_len > MAX_CONTENT and current_lines:
                chunks.append(current_lines)
                current_lines = []
                current_len = 0
            current_lines.append(line)
            current_len += line_len

        if current_lines:
            chunks.append(current_lines)

        all_ok = True
        for i, chunk_lines in enumerate(chunks):
            msg = ""
            if i == 0:
                msg += header

            table = "\n".join(chunk_lines)
            # Add footer only to the last chunk
            if i == len(chunks) - 1:
                table += f"\n{footer}"

            msg += f"```\n{table}\n```"

            if not _post(webhook_url, msg):
                all_ok = False
            time.sleep(0.5)

        return all_ok

    except Exception as e:
        print(f"⚠️ Discord Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def _post(webhook_url, content):
    """Helper to post a single message to Discord."""
    resp = requests.post(webhook_url, json={"content": content}, timeout=10)
    if resp.status_code != 204:
        print(f"❌ Discord {resp.status_code}: {resp.text}")
        return False
    return True
