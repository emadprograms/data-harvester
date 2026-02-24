"""
Diagnostic tool to verify Binance data availability in parallel.
"""
import concurrent.futures
import pandas as pd
from datetime import datetime
from src.api.binance import fetch_binance_daily
from src.utils.logger import CLILogger

BINANCE_TICKERS = [
    'BTCUSDT', 'ETHUSDT', 'PAXGUSDT'
]

logger = CLILogger()

def test_single_ticker(ticker, target_date):
    try:
        df = fetch_binance_daily(ticker, target_date, logger)
        count = len(df) if not df.empty else 0
        return {"Ticker": ticker, "Status": "✅ OK" if count > 0 else "❌ EMPTY", "Rows": count}
    except Exception as e:
        return {"Ticker": ticker, "Status": f"🚨 ERROR", "Rows": 0, "Error": str(e)}

def main():
    target_date = datetime.strptime("2026-02-23", "%Y-%m-%d").date()
    
    print(f"🚀 Testing {len(BINANCE_TICKERS)} tickers in parallel for Binance availability on {target_date}...")
    
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_ticker = {executor.submit(test_single_ticker, t, target_date): t for t in BINANCE_TICKERS}
        for future in concurrent.futures.as_completed(future_to_ticker):
            res = future.result()
            print(f"{res['Ticker']:<10} | {res['Status']} | Rows: {res['Rows']}")
            results.append(res)
            
    df_results = pd.DataFrame(results).sort_values("Ticker")
    print(f"\n📊 Summary for 2026-02-20:")
    print(df_results.groupby("Status").size())
    
    missing = df_results[df_results["Rows"] == 0]["Ticker"].tolist()
    if missing:
        print(f"\n⚠️ Tickers with NO DATA: {missing}")

if __name__ == "__main__":
    main()
