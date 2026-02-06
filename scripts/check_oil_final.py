from src.database.connection import get_db_connection

def check_final():
    client = get_db_connection()
    res = client.execute("SELECT display_name, priority_1, priority_2 FROM market_symbols WHERE display_name='CL=F'")
    if res.rows:
        print(f"CL=F Config: {res.rows[0]}")
    else:
        print("CL=F Not Found!")

if __name__ == "__main__":
    check_final()
