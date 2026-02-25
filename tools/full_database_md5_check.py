"""
Complete MD5 parity check for the ENTIRE Archive and Mirror databases.
This script compares every single record to ensure absolute 1-on-1 synchronization.
"""
import sys
import pandas as pd
from src.database.connection import get_archive_db_connection, get_mirror_db_connection
from src.utils.integrity import calculate_df_md5
from src.config import SCHEMA_COLS

def main():
    print("🚀 Initializing Full Database Parity Check...")
    
    archive_client = get_archive_db_connection()
    mirror_client = get_mirror_db_connection()
    
    if not archive_client or not mirror_client:
        print("❌ Could not connect to both databases.")
        sys.exit(1)
        
    try:
        # 1. Fetch ALL data from Archive
        print("📥 Fetching all records from ARCHIVE...")
        col_list = ", ".join(SCHEMA_COLS)
        res_a = archive_client.execute(
            f"SELECT {col_list} FROM market_data"
        )
        df_a = pd.DataFrame([list(row) for row in res_a.rows], columns=SCHEMA_COLS)
        print(f"   📊 Archive Total: {len(df_a)} rows.")
        
        # 2. Fetch ALL data from Mirror
        print("📥 Fetching all records from MIRROR...")
        res_m = mirror_client.execute(
            f"SELECT {col_list} FROM market_data"
        )
        df_m = pd.DataFrame([list(row) for row in res_m.rows], columns=SCHEMA_COLS)
        print(f"   📊 Mirror Total : {len(df_m)} rows.")
        
        # 3. Calculate MD5
        print("🧮 Calculating Master MD5 Fingerprints...")
        md5_a = calculate_df_md5(df_a)
        md5_m = calculate_df_md5(df_m)
        
        print("\n" + "="*50)
        print(f"FINAL RESULTS:")
        print(f"ARCHIVE Master MD5: {md5_a}")
        print(f"MIRROR  Master MD5: {md5_m}")
        print("="*50)
        
        if md5_a == md5_m:
            print("\n✅ ABSOLUTE PARITY CONFIRMED.")
            print("Both databases are 1-on-1 identical across all records.")
        else:
            print("\n❌ DESYNC DETECTED!")
            print("The databases are NOT identical. Run tools/sync_from_archive.py to fix.")
            
            # Additional debug info
            if len(df_a) != len(df_m):
                print(f"⚠️ Row count mismatch: Archive({len(df_a)}) vs Mirror({len(df_m)})")
            
    except Exception as e:
        print(f"❌ Error during full parity check: {e}")
    finally:
        archive_client.close()
        mirror_client.close()

if __name__ == "__main__":
    main()
