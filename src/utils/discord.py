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
    Analyze the harvest report and flag tickers with suspiciously low candle counts.
    Returns a formatted alert string for Discord, or empty string if all healthy.
    """
    if report_df.empty:
        return ""

    # Determine which sessions should have data
    applicable = []
    for threshold_hour in sorted(SESSION_AVAILABILITY.keys()):
        if now_et_hour >= threshold_hour:
            applicable = SESSION_AVAILABILITY[threshold_hour]

    if not applicable:
        return ""

    alerts = []
    for _, row in report_df.iterrows():
        ticker = row.get("Ticker", "?")
        issues = []

        for session in applicable:
            count = row.get(session, 0)
            threshold = HEALTH_THRESHOLDS.get(session, 0)
            if count < threshold and count > 0:
                issues.append(f"{session}={count} (min {threshold})")
            elif count == 0 and session in applicable:
                # 0 candles for an expected session
                issues.append(f"{session}=0 ❌")

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
                                file_path=None, health_alerts="", integrity_status=""):
    """
    Sends a compact ticker table to Discord, split across messages if needed.
    Attaches a file (e.g., local DB) if provided.
    """
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        return False

    try:
        header = f"🚜 **Harvest Complete** | `{target_date}`\n"

        if report_df.empty:
            _post(webhook_url, header + "No data harvested.")
            return True

        # Simplified message without the detailed table
        msg = header
        msg += f"📊 **Total Rows Harvested**: `{total_rows:,}`\n"
        msg += "📄 *Full details are available in the attached log.*"

        # Post the message with attachment
        all_ok = _post(webhook_url, msg, file_path)

        # Send health alerts as a follow-up message
        if health_alerts:
            if not _post(webhook_url, health_alerts):
                all_ok = False
            time.sleep(0.5)

        # Send health alerts as a follow-up message
        if health_alerts:
            if not _post(webhook_url, health_alerts):
                all_ok = False
            time.sleep(0.5)

        # Send integrity status as a follow-up message
        if integrity_status:
            if not _post(webhook_url, f"🔒 **Integrity** | {integrity_status}"):
                all_ok = False

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

