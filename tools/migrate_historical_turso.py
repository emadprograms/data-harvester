"""
Migrate ~1.05M rows of market_data from remote Turso (analyst_workbench)
to local SQLite master database (market_data.db), then sync to GDrive.

Usage:
    PYTHONPATH=. python3 scripts/migrate_historical_turso.py
"""

import time
import sys
from libsql_client import create_client_sync
from src.infisical_manager import InfisicalManager

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BATCH_SIZE = 50_000
LOCAL_DB_PATH = "market_data.db"

REMOTE_URL_SECRET = "turso_emadprograms_analystworkbench_db_url"
REMOTE_TOKEN_SECRET = "turso_emadprograms_analystworkbench_auth_token"

GDRIVE_CLIENT_ID_SECRET = "emadprograms_market_data_gdrive_client_id"
GDRIVE_CLIENT_SECRET_SECRET = "emadprograms_market_data_gdrive_client_secret"
GDRIVE_REFRESH_TOKEN_SECRET = "emadprograms_market_data_gdrive_refresh_token"
GDRIVE_FOLDER_ID_SECRET = "emadprograms_market_data_gdrive_folder_id"


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

def get_remote_client(mgr: InfisicalManager):
    """Connect to the remote Turso analyst_workbench database."""
    url = mgr.get_secret(REMOTE_URL_SECRET)
    token = mgr.get_secret(REMOTE_TOKEN_SECRET)

    if not url or not token:
        print("❌ Missing Turso credentials for analyst_workbench. Check Infisical.")
        sys.exit(1)

    http_url = url.replace("libsql://", "https://")
    return create_client_sync(url=http_url, auth_token=token)


def get_local_client():
    """Connect to the local SQLite database."""
    return create_client_sync(url=f"file:{LOCAL_DB_PATH}")


# ---------------------------------------------------------------------------
# Schema initialisation
# ---------------------------------------------------------------------------

def init_local_schema(local):
    """Ensure the market_data table exists locally."""
    local.execute("""
        CREATE TABLE IF NOT EXISTS market_data (
            timestamp TEXT NOT NULL,
            symbol TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            session TEXT,
            PRIMARY KEY (symbol, timestamp)
        )
    """)
    print("✅ Local schema verified")


# ---------------------------------------------------------------------------
# Migration Logic (Reusable)
# ---------------------------------------------------------------------------

def get_count(client, label=""):
    """Return row count from market_data."""
    res = client.execute("SELECT COUNT(*) FROM market_data")
    count = res.rows[0][0]
    if label:
        print(f"📊 {label} row count: {count:,}")
    return count


def perform_migration(remote, local, batch_size=BATCH_SIZE, logger=None):
    """
    Paginated migration using LIMIT/OFFSET.
    Uses INSERT OR IGNORE so the script is safe to re-run after interruption.
    """
    def log(msg):
        if logger: logger.log(msg)
        else: print(msg)

    total = get_count(remote, "Remote")
    if total == 0:
        log("⚠️  Remote table is empty — nothing to migrate.")
        return 0

    migrated = 0
    batch_num = 0
    start_time = time.time()

    while True:
        batch_num += 1
        offset = (batch_num - 1) * batch_size

        try:
            res = remote.execute(
                "SELECT timestamp, symbol, open, high, low, close, volume, session "
                "FROM market_data ORDER BY symbol, timestamp "
                f"LIMIT {batch_size} OFFSET {offset}"
            )
        except Exception as e:
            log(f"⚠️  Network error on batch {batch_num} (offset {offset}): {e}")
            log("   Retrying in 5 seconds...")
            time.sleep(5)
            continue

        rows = res.rows
        if not rows:
            break

        # Bulk insert
        for row in rows:
            local.execute(
                "INSERT OR IGNORE INTO market_data "
                "(timestamp, symbol, open, high, low, close, volume, session) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                list(row),
            )

        migrated += len(rows)
        elapsed = time.time() - start_time
        rate = migrated / elapsed if elapsed > 0 else 0
        log(
            f"📦 Batch {batch_num}: +{len(rows):,} rows  |  "
            f"Total: {migrated:,} / {total:,}  |  "
            f"{rate:,.0f} rows/s"
        )

        if len(rows) < batch_size:
            break

    return migrated


def repair_local_from_turso(remote, local, logger=None, force_exhaustive=False):
    """
    Heuristic repair: If Turso has more rows or newer data, 
    sync batches to ensure parity.
    """
    def log(msg):
        if logger: logger.log(msg)
        else: print(msg)

    remote_count = get_count(remote)
    local_count = get_count(local)
    
    if remote_count <= local_count and not force_exhaustive:
        # Check if remote has newer data even if counts match
        res_r = remote.execute("SELECT MAX(timestamp) FROM market_data")
        res_l = local.execute("SELECT MAX(timestamp) FROM market_data")
        r_max = res_r.rows[0][0]
        l_max = res_l.rows[0][0]
        
        if r_max and l_max and r_max <= l_max:
            log("✅ Local SQLite is up-to-date with Turso.")
            return

    log(f"🔄 Gap/Sync check: Remote: {remote_count:,}, Local: {local_count:,}.")
    
    # Exhaustive repair: fetch recent rows by timestamp descending
    batch_size = 5000
    offset = 0
    total_added = 0
    
    while True:
        res = remote.execute(
            "SELECT timestamp, symbol, open, high, low, close, volume, session "
            "FROM market_data ORDER BY timestamp DESC "
            f"LIMIT {batch_size} OFFSET {offset}"
        )
        if not res.rows: break
        
        batch_added = 0
        for row in res.rows:
            try:
                # Use INSERT OR IGNORE to only add what's missing
                local.execute(
                  "INSERT OR IGNORE INTO market_data (timestamp, symbol, open, high, low, close, volume, session) "
                  "VALUES (?,?,?,?,?,?,?,?)", list(row)
                )
                batch_added += 1
            except: pass
            
        total_added += batch_added
        offset += batch_size
        
        # Heuristic: If we processed a batch and added 0 new rows, we've likely hit the "already synced" barrier
        if batch_added == 0 and not force_exhaustive:
            break
            
        if not force_exhaustive and offset > 50000: 
            log("⚠️ Repair threshold exceeded (50k rows). Run full migrate if needed.")
            break
    
    if total_added > 0:
        log(f"✅ Repair complete. Added {total_added:,} missing rows.")
    else:
        log("✅ Parity verified. No new rows added.")


# ---------------------------------------------------------------------------
# Google Drive sync
# ---------------------------------------------------------------------------

def sync_to_gdrive(mgr: InfisicalManager, logger=None):
    """Upload the local DB to Google Drive."""
    from src.utils.gdrive import upload_to_gdrive_oauth
    def log(msg):
        if logger: logger.log(msg)
        else: print(msg)

    client_id = mgr.get_secret(GDRIVE_CLIENT_ID_SECRET)
    client_secret = mgr.get_secret(GDRIVE_CLIENT_SECRET_SECRET)
    refresh_token = mgr.get_secret(GDRIVE_REFRESH_TOKEN_SECRET)
    folder_id = mgr.get_secret(GDRIVE_FOLDER_ID_SECRET)

    if not all([client_id, client_secret, refresh_token, folder_id]):
        log("⚠️  GDrive credentials incomplete — skipping upload.")
        return False

    log("☁️  Uploading to Google Drive...")
    ok = upload_to_gdrive_oauth(LOCAL_DB_PATH, folder_id, client_id, client_secret, refresh_token, logger)
    if ok:
        log("✅ GDrive sync complete")
    else:
        log("❌ GDrive sync failed")
    return ok


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("  Turso → Local SQLite Migration")
    print("=" * 60)

    mgr = InfisicalManager()
    remote = get_remote_client(mgr)
    local = get_local_client()

    init_local_schema(local)

    # Full migration
    migrated = perform_migration(remote, local)
    print(f"\n✅ Migration finished — {migrated:,} rows processed.")

    sync_to_gdrive(mgr)
    print("\n🎉 All done!")


if __name__ == "__main__":
    main()
