# Real-Time Big Data Analytics — Project Proposal

**Project Title:** Predicting FOMC (Fed) Decision Odds from Structured Markets & Macro Data  
**Team Number:** 16

---

## Team

| Member          | NetID       |
| --------------- | ----------- |
| **Zephyr Luo**  | **zl3152**  |
| **Yushu LIU**   | **yl13841** |
| **Feifan Yang** | **fy2288**  |
| **Mark Chen**   | **jc10691** |

---

## Problem Statement & Goal

We will build a **hybrid real-time** analytics system that estimates and explains minute-level changes in the **December FOMC decision odds** (e.g., hold vs. cut/hike) by joining:

- **Prediction market probabilities** (Polymarket outcome tokens, Kalshi event contracts; optional WebSocket for sub-minute updates),
- **Equity & rates market signals** (S&P 500, VIX volatility, Treasury yields, Fed futures from yfinance),
- **Structured macro features** (FRED: CPI, unemployment, GDP, effective federal funds rate, target range),
- **Large-scale news intensity** (GDELT Events/Mentions; 15-minute cadence).

**Outputs:** a calibrated probability series, latency-to-move metrics around macro releases, and attributions of what moved the odds (prediction market shifts, equity/VIX reactions, rates curve movements, FRED macro updates, news bursts).

## Expected Insights

By combining prediction markets (Polymarket, Kalshi), equity/rates markets (yfinance), macro fundamentals (FRED), and news signals (GDELT), we aim to uncover:

- How quickly and strongly markets react to new macroeconomic information.

- Which indicators (e.g., CPI, unemployment, 10Y yields) most influence shifts in rate-cut expectations.

- The lag between real-world data releases and market repricing (“policy anticipation gap”).

- Insights useful to policy analysts, investors, and traders monitoring Fed sentiment in real time.

---

## Data Sources, Ownership, Size, and Links

_All raw goes to `/data/bronze` (CSV/JSON/ZIP); cleaned analytics tables go to `/data/silver` (Parquet+Snappy, UTC, partitioned); final features to `/data/gold`._

| #   | Dataset (link)                                                                                                                                                                                                                                                          | What we use it for                                                                                                                                                      | Cadence                                 |                 Est. size (our 4–8 week window) | Owner (MR profiling & cleaning) |
| --- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------- | ----------------------------------------------: | ------------------------------- |
| 1   | **Polymarket – "Fed decision in December"**: Event page `polymarket.com/event/fed-decision-in-december`; Link=>docs.polymarket.com/developers/CLOB/timeseries; APIs: `gamma-api.polymarket.com` (market by slug) & `clob.polymarket.com` (prices-history, book, trades) | Outcome-token **price ≈ probability**; order book (spreads/depth); trades (impact)                                                                                      | Sec–min (poll or WS); backfill via REST | **GBs+** (book snapshots every 10–15s + trades) | **Zephyr Luo**                  |
| 2   | **Kalshi – Fed Rate Decision Markets**: [https://kalshi.com](https://kalshi.com); API: `api.elections.kalshi.com`                                                                                                                                                       | Event-based prediction market probabilities for Fed rate decisions                                                                                                      | Real-time (REST API)                    |                                         **MBs** | **Zephyr Luo**                  |
| 3   | **Yahoo Finance (yfinance) – S&P 500 & Macro Market Indicators**: [https://finance.yahoo.com](https://finance.yahoo.com); Python package: `yfinance`                                                                                                                    | Stock market indices (S&P 500, NASDAQ, Dow Jones), sector ETFs, VIX volatility index, Treasury yields (10Y/5Y/3M), Fed funds futures, and other equity/macro indicators | Daily / Intraday                        |                                  **100s of MB** | **Mark Chen**                   |
| 4   | **GDELT (Global Database of Events, Language, and Tone)**: [https://www.gdeltproject.org](https://www.gdeltproject.org); API: `api.gdeltproject.org`                                                                                                                    | Large-scale news intensity and sentiment related to Federal Reserve, FOMC, rate decisions                                                                               | 15-minute cadence                       |                                         **GBs** | **Feifan Yang**                 |
| 5   | **FRED (Federal Reserve Economic Data)**: [https://fred.stlouisfed.org](https://fred.stlouisfed.org); API: `api.stlouisfed.org/fred/series/observations`                                                                                                                | Core macroeconomic indicators — CPI, unemployment, GDP, Fed funds rate, 10Y yield, and target range upper/lower limits                                                  | Daily / Monthly (series-dependent)      |                                         **MBs** | **Yushu LIU**                   |

> We combine **Polymarket & Kalshi** (prediction markets) with **FRED** (macroeconomic fundamentals), **yfinance** (equity & rates markets), and **GDELT** (large-scale news signals) to create a comprehensive real-time analytics system.

---

## Schemas & Formats (Silver / analytics-ready)

**All timestamps in UTC. Partition by `dt=YYYY-MM-DD` and `hour=HH` for time-series.**

### Polymarket

- `silver.polymarket_prices_history(token_id STRING, ts TIMESTAMP, price_cents DOUBLE, probability DOUBLE, dt STRING, hour STRING)`
- `silver.polymarket_book_top(token_id STRING, ts TIMESTAMP, best_bid_price DOUBLE, best_bid_size DOUBLE, best_ask_price DOUBLE, best_ask_size DOUBLE, spread DOUBLE, dt STRING, hour STRING)`
- `silver.polymarket_trades(ts TIMESTAMP, token_id STRING, side STRING, price DOUBLE, size DOUBLE, tx_hash STRING, dt STRING, hour STRING)`

### Kalshi

- `silver.kalshi_trades(ts TIMESTAMP, market_ticker STRING, side STRING, price DOUBLE, count INT, dt STRING, hour STRING)`
- `silver.kalshi_orderbook(market_ticker STRING, ts TIMESTAMP, yes_bid DOUBLE, yes_ask DOUBLE, no_bid DOUBLE, no_ask DOUBLE, dt STRING, hour STRING)`

### Yahoo Finance (yfinance)

- `silver.yfinance_equity(ticker STRING, date DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, adj_close DOUBLE, volume BIGINT)`
- `silver.yfinance_rates(ticker STRING, date DATE, close DOUBLE)` -- Treasury yields, Fed futures
- `silver.yfinance_vix(date DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume BIGINT)`

### GDELT (selected fields; wide → trimmed)

- `silver.gdelt_events(dt STRING, hour STRING, globaleventid BIGINT, eventcode STRING, goldsteinscale DOUBLE, nummentions INT, avgtone DOUBLE, actiongeo STRING, sourceurl STRING, ... trimmed)`
- `silver.gdelt_mentions(dt STRING, hour STRING, globaleventid BIGINT, mentionts TIMESTAMP, sourceurl STRING, ... trimmed)`

### FRED

- `silver.fred(series_id STRING, date DATE, value DOUBLE, series_label STRING)`

Each CSV corresponds to one macro series, e.g.:

```bash
fred_cpi_all_items.csv
fred_unemployment_rate.csv
fred_real_gdp.csv
fred_effective_fed_funds_rate.csv
fred_treasury_10y_yield.csv
fred_target_range_upper.csv
fred_target_range_lower.csv
```

---

## Software Architecture (Big Data Tools)

## Design Diagram

<img src="images/design_diagrams.png" alt="Design Diagram" width="60%">

---

## Usage

### Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Download all data sources (Polymarket + Kalshi + Yahoo Finance + GDELT + FRED)
python fetch_data.py
```

### Set Up Environment Variables

Create a file named .env in the project root (RDBA/.env):

```bash
FRED_API_KEY=your_fred_api_key_here
```

You can register for a free key here: https://fred.stlouisfed.org/docs/api/fred/

### Download Specific Sources

```bash
# Individual sources
python fetch_data.py -p          # Polymarket only
python fetch_data.py -k          # Kalshi only
python fetch_data.py -y          # Yahoo Finance only
python fetch_data.py -g          # GDELT news only
python fetch_data.py -f          # FRED only

# Multiple sources
python fetch_data.py -p -k       # Polymarket + Kalshi
python fetch_data.py -g -y       # GDELT + Yahoo Finance

# By source name
python fetch_data.py --source gdelt

# Custom FRED fetch
python fetch_data.py -f --fred-series CPIAUCSL,UNRATE,FEDFUNDS
python fetch_data.py -f --fred-start 2010-01-01

# List all available sources
python fetch_data.py --list
```

### Output Structure

The script creates `market_data/` directory and writes CSV exports plus metadata:

- `market_data/polymarket/` - Polymarket prediction market data (trades, orderbook, probabilities)
- `market_data/kalshi/` - Kalshi prediction market data (event contracts, trades)
- `market_data/yfinance/` - Stock market data (S&P 500, NASDAQ, Dow Jones, VIX, sector ETFs, Treasury yields, Fed futures)
- `market_data/gdelt/` - GDELT news articles and events (Federal Reserve, FOMC, rate decision related)
- `market_data/fred/` - FRED macroeconomic data (CPI, unemployment, GDP, effective Fed funds rate, target range, and other key economic indicators)

### Customizing Data Sources

Edit configuration constants in each data source module:

- **Polymarket**: `data_sources/polymarket.py` - Update `EVENT_SLUG` and `MARKET_LABELS` for different Fed decision events
- **Kalshi**: `data_sources/kalshi.py` - Update `MARKET_TICKERS` to track specific Fed rate decision contracts
- **Yahoo Finance**: `data_sources/yfinance.py` - Update `TICKERS` to include desired equity indices (^GSPC, ^IXIC, ^DJI, ^VIX), sector ETFs (XLF, XLK, etc.), Treasury yields (^TNX, ^FVX), and Fed futures; adjust `DEFAULT_START`, `INTERVAL` for historical range and frequency
- **GDELT**: `data_sources/gdelt.py` - Update `DEFAULT_QUERIES` with Fed/FOMC-related keywords, `DEFAULT_PARAMS` (timespan, maxrecords)
- **FRED**: `data_sources/fred.py` - Update `SERIES` (series IDs like CPIAUCSL, UNRATE, GDPC1, FEDFUNDS) and .env for `FRED_API_KEY`; adjust date range or output directory

### Using in Python Code

```python
from data_sources import kalshi, polymarket, yfinance, gdelt, fred

# Export from individual sources
kalshi.export_data()
polymarket.export_data()
yfinance.export_data()
gdelt.export_data()
fred.export_data()

# GDELT with custom parameters
gdelt.export_data(
    queries={"fed_decision": '("Federal Reserve" OR FOMC OR "rate cut")'},
    timespan="1month",
    maxrecords=100
)

# Or use convenience function
from data_sources import export_all
export_all(['kalshi', 'polymarket', 'gdelt', 'fred'])
```
