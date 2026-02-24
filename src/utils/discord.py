import requests
import os
import pandas as pd
import json
import time
from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# Health Alert Thresholds
# ---------------------------------------------------------------------------
# Expected minimum candle counts per session for a "healthy" ticker.
HEALTH_THRESHOLDS = {
    "Pre":  30,   # PRE: 4:00 AM – 9:30 AM ET
    "Reg":  50,   # REG: 9:30 AM – 4:00 PM ET
    "Post": 20,   # POST: 4:00 PM – 8:00 PM ET
}

# Which sessions should have data based on the current ET hour
SESSION_AVAILABILITY = {
    4:  ["Pre"],
    10: ["Pre"],
    16: ["Pre", "Reg"],
    20: ["Pre", "Reg", "Post"],
}


def build_health_alerts(report_df, now_et_hour):
    """
    Analyze the harvest report and flag tickers with suspiciously low candle counts
    or total failure.
    Skipps Pre/Post checks for Crypto/Binance assets.
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
        source = row.get("Source", "UNKNOWN")
        
        # Immediate loud alert if the ticker failed completely
        total = row.get("Total", 0)
        if total == 0:
            alerts.append(f"🚨 **{ticker:<10}** FAILED (0 Rows) ❌")
            continue
            
        # Skip Pre/Post checks for 24/7 markets (Binance)
        if "BINANCE" in source.upper():
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

    header = f"**Health Warnings** ({len(alerts)})\n"
    body = "\n".join(alerts[:15])  # Cap at 15 to avoid Discord limits
    if len(alerts) > 15:
        body += f"\n... and {len(alerts) - 15} more"

    return f"{header}```\n{body}\n```"


def send_discord_harvest_report(report_df: pd.DataFrame, target_date, total_rows,
                                file_path=None, health_alerts="", integrity_pre="", integrity_post="", 
                                critical_errors=""):
    """
    Sends a cleaned-up harvest dashboard to Discord using Embeds.
    The dashboard is sent first, followed by the log file as a separate message.
    """
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        print("⚠️ DISCORD_WEBHOOK_URL not set. Skipping notification.")
        return False

    try:
        # 1. Build the Embed Dashboard
        color = 5763719  # Green (0x57F287)
        title = f"🚜 Harvest Complete | {target_date}"
        
        if critical_errors:
            color = 15548997 # Red (0xED4245)
            title = f"🚨 Harvest Finished with Errors | {target_date}"
        elif health_alerts:
            color = 16776960 # Yellow (0xFFFF00)
            
        embed = {
            "title": title,
            "color": color,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "fields": [],
            "footer": {"text": "Data Harvester v5.0"}
        }
        
        # Overview Field
        overview_text = f"**Total Records:** `{total_rows:,}`\n"
        if not report_df.empty:
            failed_tickers = len(report_df[report_df["Total"] == 0])
            overview_text += f"**Failed Symbols:** `{failed_tickers}`\n"
        embed["fields"].append({"name": "📊 Overview", "value": overview_text, "inline": False})

        # Integrity Fields (Row 2)
        val_pre = integrity_pre if integrity_pre else "Skipped"
        val_post = integrity_post if integrity_post else "Skipped"
        
        embed["fields"].append({"name": "🔒 Pre-Harvest Parity", "value": f"`{val_pre}`", "inline": True})
        embed["fields"].append({"name": "🔒 Post-Harvest Parity", "value": f"`{val_post}`", "inline": True})

        # Critical Errors Field (if any)
        if critical_errors:
             # Truncate if too long
            err_text = (critical_errors[:1000] + '...') if len(critical_errors) > 1000 else critical_errors
            embed["fields"].append({"name": "🚨 Critical Errors", "value": f"```\n{err_text}\n```", "inline": False})

        # Health Alerts Field (if any)
        if health_alerts:
            embed["fields"].append({"name": "🏥 Health Check", "value": health_alerts, "inline": False})

        # Sources Field (if data exists)
        if not report_df.empty:
            sources = report_df["Source"].value_counts().to_dict()
            source_str = "\n".join([f"**{src}:** {count}" for src, count in sources.items()])
            embed["fields"].append({"name": "📡 Sources", "value": source_str, "inline": True})

        # 2. Step 1: Post the Dashboard Card
        dashboard_ok = _post_embed(webhook_url, embed)

        # 3. Step 2: Post the Log File
        if file_path and os.path.exists(file_path):
            time.sleep(1) # Small delay to ensure order
            file_ok = _post_file(webhook_url, "📄 **Find the attached logs below:**", file_path)
            return dashboard_ok and file_ok

        return dashboard_ok

    except Exception as e:
        print(f"⚠️ Discord Error: {e}")
        return False


def _post_embed(webhook_url, embed_dict):
    """Helper to post a single embed to Discord."""
    try:
        payload = {"embeds": [embed_dict]}
        resp = requests.post(webhook_url, json=payload, timeout=10)
        if resp.status_code not in [200, 204]:
            print(f"❌ Discord Embed Error {resp.status_code}: {resp.text}")
            return False
        return True
    except Exception as e:
        print(f"❌ Discord Post Error: {e}")
        return False

def _post_file(webhook_url, content, file_path):
    """Helper to post a file with a message to Discord."""
    try:
        with open(file_path, 'rb') as f:
            files = {'file': (os.path.basename(file_path), f)}
            resp = requests.post(webhook_url, data={'content': content}, files=files, timeout=30)
            
        if resp.status_code not in [200, 204]:
            print(f"❌ Discord File Error {resp.status_code}: {resp.text}")
            return False
        return True
    except Exception as e:
        print(f"❌ Discord Post Error: {e}")
        return False
