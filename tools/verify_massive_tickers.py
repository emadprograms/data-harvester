"""
Diagnostic tool to verify Massive (Polygon) data availability for a specific date.
"""
import asyncio
import pandas as pd
from datetime import datetime
from src.api.massive import fetch_massive_data
from src.utils.logger import CLILogger

MASSIVE_TICKERS = [
    'AAPL', 'ADBE', 'AMD', 'AMZN', 'APP', 'AVGO', 'BABA', 'DIA', 'GOOGL', 
    'IWM', 'LRCX', 'META', 'MSFT', 'MU', 'NDAQ', 'NVDA', 'ORCL', 'PANW', 
    'PAXGUSDT', 'QCOM', 'QQQ', 'SHOP', 'SMH', 'SPY', 'TLT', 'TSLA', 'TSM', 
    'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLU', 'XLV'
]

async def test_tickers():
    logger = CLILogger()
    target_date = datetime.strptime("2026-02-23", "%Y-%m-%d").date()
    
    print(f"🔍 Testing {len(MASSIVE_TICKERS)} tickers for Massive availability on {target_date}...")
    
    results = []
    for ticker in MASSIVE_TICKERS:
        try:
            # fetch_massive_data is sync
            df = fetch_massive_data(ticker, target_date, logger)
            count = len(df) if not df.empty else 0
            status = "✅ OK" if count > 0 else "❌ EMPTY"
            print(f"{ticker:<10} | {status} | Rows: {count}")
            results.append({"Ticker": ticker, "Status": status, "Rows": count})
        except Exception as e:
            print(f"{ticker:<10} | 🚨 ERROR: {e}")
            results.append({"Ticker": ticker, "Status": "ERROR", "Rows": 0})
            
    df_results = pd.DataFrame(results)
    print("\n📊 Summary:")
    print(df_results.groupby("Status").size())

if __name__ == "__main__":
    asyncio.run(test_tickers())
