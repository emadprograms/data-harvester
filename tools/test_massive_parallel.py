"""
Diagnostic tool to verify Massive (Polygon) data availability using parallel keys.
"""
import concurrent.futures
import pandas as pd
import os
from datetime import datetime
from src.api.massive import MassiveProvider
from src.utils.logger import CLILogger

# Removed PAXGUSDT as it is a Binance ticker
MASSIVE_TICKERS = [
    'AAPL', 'ADBE', 'AMD', 'AMZN', 'APP', 'AVGO', 'BABA', 'DIA', 'GOOGL', 
    'IWM', 'LRCX', 'META', 'MSFT', 'MU', 'NDAQ', 'NVDA', 'ORCL', 'PANW', 
    'QCOM', 'QQQ', 'SHOP', 'SMH', 'SPY', 'TLT', 'TSLA', 'TSM', 
    'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLU', 'XLV'
]

# We use a global provider to ensure round-robin rotation works across threads
logger = CLILogger()
provider = MassiveProvider(logger)

def test_single_ticker(ticker, target_date):
    try:
        # provider.fetch_data rotates keys properly
        df = provider.fetch_data(ticker, target_date)
        count = len(df) if not df.empty else 0
        return {"Ticker": ticker, "Status": "✅ OK" if count > 0 else "❌ EMPTY", "Rows": count}
    except Exception as e:
        return {"Ticker": ticker, "Status": f"🚨 ERROR", "Rows": 0, "Error": str(e)}

def main():
    # Testing for the requested date 2026-02-23
    target_date = datetime.strptime("2026-02-23", "%Y-%m-%d").date()
    
    print(f"🚀 Testing {len(MASSIVE_TICKERS)} tickers in parallel for {target_date}...")
    print(f"🔑 Using shared provider with {len(provider.clients)} rotating keys.")
    
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_ticker = {executor.submit(test_single_ticker, t, target_date): t for t in MASSIVE_TICKERS}
        for future in concurrent.futures.as_completed(future_to_ticker):
            res = future.result()
            print(f"{res['Ticker']:<10} | {res['Status']} | Rows: {res['Rows']}")
            results.append(res)
            
    df_results = pd.DataFrame(results).sort_values("Ticker")
    print(f"\n📊 Summary for {target_date}:")
    print(df_results.groupby("Status").size())
    
    missing = df_results[df_results["Rows"] == 0]["Ticker"].tolist()
    if missing:
        print(f"\n⚠️ Tickers with NO DATA: {missing}")

if __name__ == "__main__":
    main()
