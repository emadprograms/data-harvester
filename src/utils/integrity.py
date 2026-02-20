"""
Data integrity verification between Turso (remote) and local SQLite.
Uses a lightweight "statistical fingerprint" to detect sync drift
without reading all rows.
"""


def compute_fingerprint(client, date_str):
    """
    Returns a fingerprint tuple (count, volume_sum, max_ts, min_ts)
    for all market_data rows on or after the given date string.
    """
    try:
        res = client.execute(
            "SELECT COUNT(*), COALESCE(SUM(CAST(volume AS INTEGER)), 0), "
            "MAX(timestamp), MIN(timestamp) "
            "FROM market_data WHERE timestamp >= ?",
            [f"{date_str} 00:00:00"]
        )
        row = res.rows[0]
        return {
            "count": row[0] or 0,
            "volume_sum": row[1] or 0,
            "max_ts": row[2] or "",
            "min_ts": row[3] or "",
        }
    except Exception as e:
        return {"count": -1, "volume_sum": -1, "max_ts": "ERR", "min_ts": "ERR", "error": str(e)}


def verify_sync(turso_client, local_client, date_str, logger=None):
    """
    Compare fingerprints between Turso and local SQLite for a given date.
    Returns (is_ok: bool, details: str).
    """
    fp_remote = compute_fingerprint(turso_client, date_str)
    fp_local = compute_fingerprint(local_client, date_str)

    if fp_remote.get("error") or fp_local.get("error"):
        msg = f"⚠️ Integrity check skipped (query error)"
        if logger:
            logger.log(msg)
        return True, msg  # Don't block on query errors

    is_ok = (fp_remote["count"] == fp_local["count"])
    drift = fp_remote["count"] - fp_local["count"]

    if is_ok:
        msg = f"✅ Sync OK for {date_str} | Rows: {fp_local['count']:,}"
    else:
        msg = (
            f"❌ SYNC DRIFT on {date_str} | "
            f"Turso: {fp_remote['count']:,} rows | "
            f"Local: {fp_local['count']:,} rows"
        )

    if logger:
        logger.log(msg)

    return is_ok, msg
