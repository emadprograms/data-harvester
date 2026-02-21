"""Check when the corrupted data was written — is it old data from before the date filter fix?"""
from src.database.connection import get_db_connection
from src.infisical_manager import InfisicalManager

mgr = InfisicalManager()
client = get_db_connection()

# AAPL close=438.88 rows
res = client.execute("""
    SELECT timestamp, symbol, open, close, session
    FROM market_data 
    WHERE symbol = 'AAPL'
    AND close BETWEEN 438.0 AND 440.0
    ORDER BY timestamp
    LIMIT 10
""")
print("=== AAPL rows with close ~438.88 ===")
for row in res.rows:
    print(f"  {row[0]} | {row[1]} | open={row[2]} | close={row[3]} | {row[4]}")

# What symbol naturally has close ~438.88?
# Check APP which was ~438 in the earlier query
res2 = client.execute("""
    SELECT timestamp, symbol, open, close, session
    FROM market_data 
    WHERE symbol = 'APP'
    AND close BETWEEN 438.0 AND 440.0
    ORDER BY timestamp
    LIMIT 5
""")
print("\n=== APP rows with close ~438.88 ===") 
for row in res2.rows:
    print(f"  {row[0]} | {row[1]} | open={row[2]} | close={row[3]} | {row[4]}")

# Check the TOTAL count of rows per (timestamp, session) for 2026-02-20 10:19:00
res3 = client.execute("""
    SELECT COUNT(*), COUNT(DISTINCT close) 
    FROM market_data 
    WHERE timestamp = '2026-02-20 10:19:00'
""")
print(f"\n=== At timestamp 2026-02-20 10:19:00 ===")
print(f"  Total symbols: {res3.rows[0][0]}, Distinct close values: {res3.rows[0][1]}")

# What does the CORRECT AAPL data look like on Feb 20?
res4 = client.execute("""
    SELECT timestamp, close, session 
    FROM market_data 
    WHERE symbol = 'AAPL'
    AND timestamp >= '2026-02-20 14:30:00' 
    AND timestamp <= '2026-02-20 14:35:00'
    ORDER BY timestamp
""")
print("\n=== AAPL Regular Session (Feb 20, 14:30-14:35 UTC = 9:30-9:35 ET) ===")
for row in res4.rows:
    print(f"  {row[0]} | close={row[1]} | {row[2]}")

client.close()
