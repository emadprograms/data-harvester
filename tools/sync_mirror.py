"""
Incremental Mirror Sync — syncs Archive → Mirror for dirty days only.

This script is designed to be run via GitHub Actions (weekly) or manually.
It conserves Turso reads/writes by only syncing days where Archive has
more rows than Mirror (i.e., new data was written).

Strategy:
  1. Fetch distinct dates with data from both Archive and Mirror.
  2. Compare row counts per date.
  3. For "dirty" dates (Archive has more rows), delete+re-insert from Archive.
  4. Also full-replaces symbol_map (small table, always safe).
"""
import sys
import os

# Ensure project root is on the path when run standalone
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.connection import get_archive_db_connection, get_mirror_db_connection
from src.database.schema import _init_client
from src.database.operations import _save_to_client
from src.config import SCHEMA_COLS


class SyncLogger:
    """Simple logger that prints to stdout for GitHub Actions visibility."""
    def log(self, msg):
        print(msg)


def get_date_row_counts(client):
    """Returns a dict of {date_str: row_count} for all dates in market_data."""
    try:
        res = client.execute(
            "SELECT DATE(timestamp) as dt, COUNT(*) as cnt FROM market_data GROUP BY DATE(timestamp)"
        )
        return {row[0]: row[1] for row in res.rows}
    except Exception as e:
        print(f"❌ Error fetching date counts: {e}")
        return {}


def sync_symbol_map(archive, mirror, logger):
    """Full-replaces symbol_map in Mirror from Archive."""
    logger.log("📋 Syncing symbol_map...")
    
    try:
        res = archive.execute(
            "SELECT display_name, yahoo_ticker, massive_ticker, binance_ticker, capital_ticker FROM symbol_map"
        )
        
        if not res.rows:
            logger.log("   ⚠️ Archive symbol_map is empty. Skipping.")
            return
        
        # Clear and re-insert
        mirror.execute("DELETE FROM symbol_map")
        for row in res.rows:
            mirror.execute(
                "INSERT INTO symbol_map (display_name, yahoo_ticker, massive_ticker, binance_ticker, capital_ticker) VALUES (?, ?, ?, ?, ?)",
                list(row)
            )
        
        logger.log(f"   ✅ Synced {len(res.rows)} symbols to Mirror.")
    except Exception as e:
        logger.log(f"   ❌ symbol_map sync failed: {e}")


def sync_dirty_days(archive, mirror, logger):
    """
    Compares row counts per date between Archive and Mirror.
    For dates where Archive has more rows, re-syncs that date.
    """
    logger.log("📊 Comparing date-level row counts...")
    
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
            # 1. Delete this date from Mirror
            mirror.execute(
                "DELETE FROM market_data WHERE DATE(timestamp) = ?",
                [date_str]
            )
            
            # 2. Fetch from Archive
            res = archive.execute(
                f"SELECT {col_list} FROM market_data WHERE DATE(timestamp) = ?",
                [date_str]
            )
            
            if not res.rows:
                logger.log(f"      ⚠️ No rows returned from Archive for {date_str}. Skipping.")
                continue
            
            # 3. Insert into Mirror
            rows_to_insert = [tuple(row) for row in res.rows]
            _save_to_client(mirror, rows_to_insert, logger, f"Mirror({date_str})")
            total_rows_synced += len(rows_to_insert)
            
        except Exception as e:
            logger.log(f"      ❌ Error syncing {date_str}: {e}")
    
    return len(dirty_dates), total_rows_synced


def main():
    logger = SyncLogger()
    logger.log("=" * 60)
    logger.log("🔄 Mirror Sync — Incremental Dirty-Day Strategy")
    logger.log("=" * 60)
    
    archive = get_archive_db_connection()
    mirror = get_mirror_db_connection()
    
    if not archive or not mirror:
        logger.log("❌ Could not connect to both databases.")
        sys.exit(1)
    
    try:
        # Ensure Mirror schema exists
        _init_client(mirror)
        
        # 1. Sync symbol_map (always full replace, tiny table)
        sync_symbol_map(archive, mirror, logger)
        
        # 2. Sync dirty days (incremental)
        days_synced, rows_synced = sync_dirty_days(archive, mirror, logger)
        
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
