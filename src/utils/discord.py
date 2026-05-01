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
            
        # Skip Pre/Post checks for 24/7 markets (Binance / Crypto)
        # Check both source AND ticker name to handle fallback scenarios
        # (e.g. crypto falling back to Yahoo should still skip Pre/Post)
        if "BINANCE" in source.upper() or ticker.endswith("USDT"):
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

def build_database_health_grid(db_counts, inventory_list, session_start_utc, session_end_utc, is_active_session=False):
    """
    Builds a visual grid representing the actual database data coverage for the targeted session.
    Adjusts expectations dynamically based on elapsed time if the session is active.
    Follows a strict 5-step square emoji scale.
    """
    if not inventory_list:
        return ""
        
    grid = []
    sorted_inventory = sorted(inventory_list)
    
    now_utc = datetime.now(timezone.utc)
    
    # --- DYNAMIC EXPECTATION CALCULATION ---
    # Total minutes in the session range
    total_session_mins = int((session_end_utc - session_start_utc).total_seconds() // 60)
    
    # If the session is complete (or we are past its end time), use the full duration.
    # Otherwise, use the elapsed time from the start of the session to *now*.
    if is_active_session and now_utc < session_end_utc:
        # Cap elapsed time at the session bounds
        actual_end = max(session_start_utc, now_utc)
    else:
        actual_end = session_end_utc
        
    # Crypto: Expects 1 candle per minute, 24/7.
    elapsed_crypto_mins = int((actual_end - session_start_utc).total_seconds() // 60)
    crypto_expected = max(1, elapsed_crypto_mins)

    # Equities: Only trade 16 hours a day (4:00 AM ET to 8:00 PM ET).
    # We must calculate how many "trading minutes" have actually passed in the requested window.
    import pytz
    eastern = pytz.timezone('US/Eastern')
    
    # Walk through the session minute by minute (or hour by hour) to count valid trading minutes.
    # Since sessions are exactly 8 PM to 8 PM ET (1440 mins), we can optimize this.
    # A full normal 24h session has exactly 960 trading minutes.
    # If it's a 72h weekend session (Friday 8 PM to Monday 8 PM), only Monday has trading minutes.
    # To keep it simple and accurate dynamically:
    
    equity_expected = 0
    if not is_active_session:
        # Completed session: Assume max possible for the session type.
        # A standard session has 960. We use 960 as the baseline for "100% full".
        equity_expected = 960
    else:
        # Active session: We need to know how many trading minutes have passed since the session *started*.
        # The session starts at 8:00 PM ET (which is closed). Trading begins at 4:00 AM ET the *next* day.
        # So we just convert actual_end to ET and see how far into the trading day we are.
        end_et = actual_end.astimezone(eastern)
        
        # Determine the target trading day (the day the session ends on)
        trading_day = session_end_utc.astimezone(eastern).date()
        
        start_of_trading = eastern.localize(datetime.combine(trading_day, datetime.strptime("04:00", "%H:%M").time()))
        end_of_trading = eastern.localize(datetime.combine(trading_day, datetime.strptime("20:00", "%H:%M").time()))
        
        if end_et < start_of_trading:
            equity_expected = 1 # Prevent div by zero, market hasn't opened yet
        elif end_et > end_of_trading:
            equity_expected = 960
        else:
            equity_expected = max(1, int((end_et - start_of_trading).total_seconds() // 60))


    for symbol in sorted_inventory:
        count = db_counts.get(symbol, 0)
        
        is_crypto = symbol.endswith("USDT") or "PAXG" in symbol or symbol == "UUP"
        expected = crypto_expected if is_crypto else equity_expected
        
        # Calculate percentage of expected
        pct = (count / expected) * 100 if expected > 0 else 0
        
        if pct >= 65:
            emoji = "🟩" 
        elif pct >= 40:
            emoji = "🟨" 
        elif pct >= 15:
            emoji = "🟧" 
        elif count > 0:
            emoji = "🟥" 
        else:
            emoji = "⬛" 
            
        grid.append(f"`{symbol:<8}` {emoji} `({count})`")
        
    # Group into columns
    columns = 2
    rows = []
    for i in range(0, len(grid), columns):
        row = " | ".join(grid[i:i+columns])
        rows.append(row)
        
    grid_str = "\n".join(rows)
    
    if len(grid_str) > 950:
        grid_str = grid_str[:900] + "\n... (truncated)"
        
    legend = "🟩 >65% | 🟨 >40% | 🟧 >15% | 🟥 >0% | ⬛ 0"
    return f"{legend}\n{grid_str}"

def send_discord_harvest_report(report_df: pd.DataFrame, target_date, total_rows,
                                file_path=None, health_alerts="",
                                critical_errors="", db_health_grid=""):
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


        # Database Health Grid Field (New)
        if db_health_grid:
            embed["fields"].append({"name": "🗄️ Actual Database Health (Session Coverage)", "value": db_health_grid, "inline": False})

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
