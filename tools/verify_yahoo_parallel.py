"""
Diagnostic tool to verify Yahoo Finance data availability in parallel.
"""
import concurrent.futures
import pandas as pd
from datetime import datetime
from src.api.yahoo import fetch_yahoo_market_data
from src.utils.logger import CLILogger

YAHOO_TICKERS = [
    'AAPL', 'ADBE', 'AMD', 'AMZN', 'APP', 'AVGO', 'BABA', 'BTC-USD', 'CL=F', 
    'DIA', 'ETH-USD', 'EURUSD=X', 'GC=F', 'GOOGL', 'IWM', 'LRCX', 'META', 
    'MSFT', 'MU', 'NDAQ', 'NVDA', 'ORCL', 'PANW', 'PAXG-USD', 'QCOM', 
    'QQQ', 'SHOP', 'SMH', 'SPY', 'TLT', 'TSLA', 'TSM', 'UUP', '^VIX', 
    'XLC', 'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLU', 'XLV'
]

logger = CLILogger()

def test_single_ticker(ticker, target_date):
    try:
        df = fetch_yahoo_market_data(ticker, target_date, logger)
        count = len(df) if not df.empty else 0
        return {"Ticker": ticker, "Status": "✅ OK" if count > 0 else "❌ EMPTY", "Rows": count}
    except Exception as e:
        return {"Ticker": ticker, "Status": f"🚨 ERROR", "Rows": 0, "Error": str(e)}

def main():
    target_date = datetime.strptime("2026-02-23", "%Y-%m-%d").date()
    
    print(f"🚀 Testing {len(YAHOO_TICKERS)} tickers in parallel for Yahoo availability on {target_date}...")
    
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_ticker = {executor.submit(test_single_ticker, t, target_date): t for t in YAHOO_TICKERS}
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
