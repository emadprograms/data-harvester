import requests
import os
import pandas as pd
import time
from datetime import datetime, time as dt_time


# ---------------------------------------------------------------------------
# Health Alert Thresholds
# ---------------------------------------------------------------------------
# Expected minimum candle counts per session for a "healthy" ticker.
# These are conservative minimums — anything below flags a warning.
HEALTH_THRESHOLDS = {
    "Pre":  30,   # PRE: 4:00 AM – 9:30 AM ET (~330 min, but low-volume tickers have fewer)
    "Reg":  50,   # REG: 9:30 AM – 4:00 PM ET (~390 min)
    "Post": 20,   # POST: 4:00 PM – 8:00 PM ET (~240 min)
}

# Which sessions should have data based on the current ET hour
SESSION_AVAILABILITY = {
    # hour: [sessions that should have data by this hour]
    4:  ["Pre"],
    10: ["Pre"],
    16: ["Pre", "Reg"],
    20: ["Pre", "Reg", "Post"],
}


def build_health_alerts(report_df, now_et_hour):
    """
    Analyze the harvest report and flag tickers with suspiciously low candle counts
    or total failure.
    Returns a formatted alert string for Discord, or empty string if all healthy.
    """
    if report_df.empty:
        return ""

    # Determine which sessions should have data
    applicable = []
    if now_et_hour >= 4:
        applicable.append("Pre")
    if now_et_hour >= 10: 
        applicable.append("Reg")
    if now_et_hour >= 16:
        applicable.append("Post")

    alerts = []
    for _, row in report_df.iterrows():
        ticker = row.get("Ticker", "?")
        
        # Immediate loud alert if the ticker failed completely
        total = row.get("Total", 0)
        if total == 0:
            alerts.append(f"🚨 **{ticker:<10}** FAILED (0 Rows Harvested) ❌")
            continue
            
        issues = []
        for session in applicable:
            count = row.get(session, 0)
            threshold = HEALTH_THRESHOLDS.get(session, 0)
            
            if count == 0:
                issues.append(f"{session}=0❌")
            elif count < threshold:
                issues.append(f"{session}={count} (low)")

        if issues:
            alerts.append(f"⚠️ {ticker:<10} {', '.join(issues)}")

    if not alerts:
        return ""

    header = f"🏥 **Health Check** ({len(alerts)} warnings)\n"
    body = "\n".join(alerts[:15])  # Cap at 15 to avoid Discord limits
    if len(alerts) > 15:
        body += f"\n... and {len(alerts) - 15} more"

    return f"{header}```\n{body}\n```"


def send_discord_harvest_report(report_df: pd.DataFrame, target_date, total_rows,
                                file_path=None, health_alerts="", integrity_status="", 
                                critical_errors=""):
    """
    Sends a complete harvest report to Discord, including the ticker summary table.
    """
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        print("⚠️ DISCORD_WEBHOOK_URL not set. Skipping notification.")
        return False

    try:
        header = f"🚜 **Harvest Complete** | `{target_date}`\n"
        
        # Add critical errors at the top if present
        if critical_errors:
            msg = f"🚨 **CRITICAL ERRORS DETECTED** 🚨\n{critical_errors}\n\n" + header
        else:
            msg = header

        if report_df.empty and not critical_errors:
            _post(webhook_url, msg + "No data harvested.")
            return True

        # Metadata
        msg += f"📊 **Total Rows Harvested**: `{total_rows:,}`\n"
        
        # --- Harvest Summary Table (The Dashboard) ---
        if not report_df.empty:
            # We assume report_df has: Ticker, Source, Total, Status
            # If it has session columns (Pre, Reg, Post), we include those too
            summary_cols = ["Ticker", "Source", "Total"]
            # Dynamically check for session columns
            for s in ["Pre", "Reg", "Post"]:
                if s in report_df.columns:
                    summary_cols.append(s)
            
            # Format as a code block table
            table_str = report_df[summary_cols].to_string(index=False)
            msg += f"```\n{table_str}\n```"

        # Alerts and Integrity
        if health_alerts:
            msg += f"\n{health_alerts}"
            
        if integrity_status:
            msg += f"\n🔒 **Integrity** | {integrity_status}"

        # Truncate to avoid 2000-character Discord limit
        if len(msg) > 1950:
            msg = msg[:1950] + "\n... (truncated)"

        # Post the message with the log file attachment
        all_ok = _post(webhook_url, msg, file_path)

        return all_ok

    except Exception as e:
        print(f"⚠️ Discord Error: {e}")
        return False


def _post(webhook_url, content, file_path=None):
    """Helper to post content and optional file to Discord."""
    try:
        if file_path and os.path.exists(file_path):
            with open(file_path, 'rb') as f:
                files = {'file': (os.path.basename(file_path), f)}
                # Note: When sending files, the content goes into the 'content' field of data
                resp = requests.post(webhook_url, data={'content': content}, files=files, timeout=30)
        else:
            resp = requests.post(webhook_url, json={'content': content}, timeout=10)
            
        if resp.status_code not in [200, 204]:
            print(f"❌ Discord {resp.status_code}: {resp.text}")
            return False
        return True
    except Exception as e:
        print(f"❌ Discord Post Error: {e}")
        return False
