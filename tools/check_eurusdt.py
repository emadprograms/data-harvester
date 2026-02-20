
from src.database.connection import get_db_connection
from src.database.operations import get_symbol_map_from_db

def check_eurusdt():
    conn = get_db_connection()
    if not conn:
        print("Failed to connect")
        return
    symbols = get_symbol_map_from_db(conn)
    if "EURUSDT" in symbols:
        print(f"EURUSDT rules: {symbols['EURUSDT']}")
    else:
        print("EURUSDT not found")
    conn.close()

if __name__ == "__main__":
    check_eurusdt()
