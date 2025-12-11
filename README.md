# Market Data Harvester 🦁

A robust Streamlit application for harvesting, normalizing, and storing stock market data from multiple sources (**Yahoo Finance** & **Capital.com**). It supports hybrid data collection strategies and stores results in a **Turso (libSQL)** database.

## 🚀 Features

*   **Binance Lane (Crypto):**
    *   Fetches 24h data from **Binance**.
    *   **Smart Fallback:** If Binance fails (e.g. invalid symbol in region), it automatically falls back to **Yahoo Finance** (mapping `EURUSDT` -> `EURUSD=X` etc).
*   **Stock Lane (Hybrid):**
    *   Fetches **Regular Session** (9:30 AM - 4:00 PM ET) data from **Yahoo Finance**.
    *   Fetches **Pre-Market** (4:00 AM - 9:30 AM ET) and **Post-Market** (4:00 PM - 8:00 PM ET) data from **Capital.com**.
    *   **Smart Fallback:** If Yahoo Finance fails, it automatically falls back to Capital.com.
*   **Inventory Manager:** easily add, edit, or remove stock tickers and map them to Capital.com "Epics".
*   **Glass Box Dashboard:** Real-time, visual status matrix showing exactly what the harvester is doing (Source, Status, Row Counts).
*   **Data Health Dashboard:** Visualize your data completeness with a heat-map calendar.
*   **Database:** Stores all 1-minute OHLCV data in a Turso database.

## 🛠️ Setup & Installation

### 1. Prerequisites
*   Python 3.12+ (Recommended)
*   A **Turso** Database (libSQL)
*   A **Capital.com** API Account (Live or Demo)

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configuration (`.streamlit/secrets.toml`)
Create a `.streamlit/secrets.toml` file in the root directory. This file holds your sensitive credentials.

```toml
[turso]
db_url = "libsql://your-database-name.turso.io"
auth_token = "your-turso-auth-token"

[capital_com]
X_CAP_API_KEY = "your-capital-api-key"
identifier = "your-email-or-login-id"
password = "your-password"
```

> **Note:** For CLI usage or deployment where `secrets.toml` is not available, you can set these as Environment Variables:
> *   `TURSO_DB_URL`, `TURSO_AUTH_TOKEN`
> *   `CAPITAL_X_CAP_API_KEY`, `CAPITAL_IDENTIFIER`, `CAPITAL_PASSWORD`

## 🖥️ Usage

### Run the App
```bash
streamlit run app.py
```

### 1. 🌱 Data Harvester (Default)
*   Select the tickers you want to harvest (Defaults to All).
*   Choose your **Harvest Mode** (Full Day, Pre-Market Only, etc.).
*   Click **Start Harvest**.
*   Watch the "Glass Box" dashboard update in real-time.
*   Once finished, review the **Report Card** and click **Commit Data** to save to Turso.

### 2. ⚙️ Inventory Manager
*   **Add New Symbol:** Enter the Ticker (e.g., `NVDA`) and the Capital.com Epic (e.g., `NVDA`).
*   **Strategy:**
    *   **HYBRID:** Uses Yahoo for Regular session, Capital for Pre/Post. (Best for Stocks/ETFs).
    *   **CAPITAL_ONLY:** Uses Capital.com for everything. (Best for Indices/CFDs like `US500`).

### 3. 🗓️ Data Health Dashboard
*   Select a month and year.
*   View a heatmap of collected candles per day to identify gaps in your data.

## 🤖 Automation (CLI)

You can run the harvester without the UI (e.g., for cron jobs or GitHub Actions) using the CLI script.

```bash
python harvest_cli.py
```

This script will:
1.  Fetch all symbols in your inventory.
2.  Harvest "Full Day" data for today.
3.  Automatically save the results to the database.
