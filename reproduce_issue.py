import requests

domains = ["https://api.binance.us"]
symbols = ["EURUSDT", "EURUSD", "BTCUSDT"]

for domain in domains:
    for symbol in symbols:
        url = f"{domain}/api/v3/exchangeInfo"
        try:
            print(f"Checking {symbol} on {domain}...")
            response = requests.get(url, params={"symbol": symbol}, timeout=5)
            print(f"Status: {response.status_code}")
            if response.status_code == 200:
                print(f"Found {symbol} on {domain}")
            else:
                print(f"Response: {response.text}")
        except Exception as e:
            print(f"Error connecting to {domain}: {e}")
