"""
Incremental Mirror Sync — syncs Archive → Mirror using Native Double-Replica Caching.

This script runs weekly via GitHub Actions or manually.
It uses two local embedded replicas (archive_local.db and mirror_local.db)
to run all analysis and row scans locally, using 0 Turso reads.
"""
import sys
import os

# Ensure project root is on the path when run standalone
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.connection import get_archive_embedded_connection, get_mirror_embedded_connection
from src.database.operations import _save_to_client
from src.config import SCHEMA_COLS


class SyncLogger:
    """Simple logger that prints to stdout for GitHub Actions visibility."""
    def log(self, msg):
        print(msg)


def get_date_row_counts(client):
    """Returns a dict of {date_str: row_count} for all dates in market_data."""
    try:
        # Executes locally on disk (0 Turso reads)
        res = client.execute(
            "SELECT DATE(timestamp) as dt, COUNT(*) as cnt FROM market_data GROUP BY DATE(timestamp)"
        )
        return {row[0]: row[1] for row in res.fetchall()}
    except Exception as e:
        print(f"❌ Error fetching date counts: {e}")
        return {}


def sync_symbol_map(archive, mirror, logger):
    """Full-replaces symbol_map in Mirror from Archive locally."""
    logger.log("📋 Syncing symbol_map...")
    
    try:
        # Fetch from local archive replica (0 Turso reads)
        res = archive.execute(
            "SELECT display_name, yahoo_ticker, massive_ticker, binance_ticker, capital_ticker FROM symbol_map"
        )
        
        rows = res.fetchall()
        if not rows:
            logger.log("   ⚠️ Archive symbol_map is empty. Skipping.")
            return
        
        # Clear and re-insert into local mirror replica
        mirror.execute("DELETE FROM symbol_map")
        for row in rows:
            mirror.execute(
                "INSERT INTO symbol_map (display_name, yahoo_ticker, massive_ticker, binance_ticker, capital_ticker) VALUES (?, ?, ?, ?, ?)",
                list(row)
            )
        
        logger.log(f"   ✅ Synced {len(rows)} symbols locally.")
    except Exception as e:
        logger.log(f"   ❌ symbol_map sync failed: {e}")


def sync_dirty_days(archive, mirror, logger):
    """
    Compares row counts per date between local Archive and Mirror.
    For dates where Archive has more rows, re-syncs that date locally.
    """
    logger.log("📊 Comparing date-level row counts locally...")
    
    # Query locally (0 Turso reads)
    archive_counts = get_date_row_counts(archive)
    mirror_counts = get_date_row_counts(mirror)
    
    if not archive_counts:
        logger.log("   ⚠️ Archive has no data. Nothing to sync.")
        return 0, 0
    
    # Find dirty days: Archive has more rows than Mirror for that date
    dirty_dates = []
    for date_str, archive_count in archive_counts.items():
        mirror_count = mirror_counts.get(date_str, 0)
        if archive_count > mirror_count:
            dirty_dates.append((date_str, archive_count, mirror_count))
    
    total_dates = len(archive_counts)
    clean_dates = total_dates - len(dirty_dates)
    
    logger.log(f"   📅 Total dates in Archive: {total_dates}")
    logger.log(f"   ✅ Clean (already synced): {clean_dates}")
    logger.log(f"   🔄 Dirty (need sync): {len(dirty_dates)}")
    
    if not dirty_dates:
        logger.log("   🎉 Mirror is fully up to date. No sync needed.")
        return 0, 0
    
    # Sync each dirty date
    total_rows_synced = 0
    col_list = ", ".join(SCHEMA_COLS)
    
    for date_str, arch_count, mirr_count in sorted(dirty_dates):
        logger.log(f"\n   🔄 Syncing {date_str} (Archive: {arch_count}, Mirror: {mirr_count})...")
        
        try:
            # 1. Delete this date from local Mirror replica (automatically sent to remote primary)
            mirror.execute(
                "DELETE FROM market_data WHERE DATE(timestamp) = ?",
                [date_str]
            )
            
            # 2. Fetch from local Archive replica (0 Turso reads)
            res = archive.execute(
                f"SELECT {col_list} FROM market_data WHERE DATE(timestamp) = ?",
                [date_str]
            )
            
            rows = res.fetchall()
            if not rows:
                logger.log(f"      ⚠️ No rows returned from Archive for {date_str}. Skipping.")
                continue
            
            # 3. Insert into local Mirror replica (automatically sent to remote primary)
            rows_to_insert = [tuple(row) for row in rows]
            _save_to_client(mirror, rows_to_insert, logger, f"Mirror({date_str})")
            total_rows_synced += len(rows_to_insert)
            
        except Exception as e:
            logger.log(f"      ❌ Error syncing {date_str}: {e}")
    
    return len(dirty_dates), total_rows_synced


def main():
    logger = SyncLogger()
    logger.log("=" * 60)
    logger.log("🔄 Mirror Sync — Native Double-Replica Caching Strategy")
    logger.log("=" * 60)
    
    # 1. Open local embedded replicas
    archive = get_archive_embedded_connection()
    mirror = get_mirror_embedded_connection()
    
    if not archive or not mirror:
        logger.log("❌ Could not connect to both database replicas.")
        sys.exit(1)
    
    try:
        # 2. Trigger native binary page synchronization (fetches only new changes)
        logger.log("📥 Syncing local Archive replica...")
        archive.sync()
        logger.log("📥 Syncing local Mirror replica...")
        mirror.sync()
        
        # 3. Sync symbol_map locally
        sync_symbol_map(archive, mirror, logger)
        
        # 4. Sync dirty days locally (costs 0 Turso reads for comparison and fetching)
        days_synced, rows_synced = sync_dirty_days(archive, mirror, logger)
        
        # 5. Native sync to push any pending writes to the remote Mirror DB
        logger.log("\n📤 Pushing sync writes to remote Mirror DB...")
        mirror.sync()
        
        # Summary
        logger.log("\n" + "=" * 60)
        logger.log("📋 SYNC SUMMARY")
        logger.log(f"   Days synced:  {days_synced}")
        logger.log(f"   Rows written: {rows_synced:,}")
        logger.log("=" * 60)
        
        if days_synced == 0:
            logger.log("✅ Mirror is fully up to date.")
        else:
            logger.log(f"✅ Successfully synced {days_synced} dirty day(s) to Mirror.")
            
    except Exception as e:
        import traceback
        logger.log(f"❌ Sync failed: {e}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        try: archive.close()
        except: pass
        try: mirror.close()
        except: pass


if __name__ == "__main__":
    main()
