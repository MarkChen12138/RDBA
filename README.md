# Real-Time Big Data Analytics — Project Proposal

**Project Title:** Predicting FOMC (Fed) Decision Odds from Structured Markets & Macro Data  
**Team Number:** 16

---

## Team

| Member                    | NetID      | 
| ------------------------- | ---------- | 
| **Zephyr Luo**            | **zl3152** | 
| **Yushu LIU**             | **yl13841** | 
| **<Member 3>**            | <netid>    | 
| **<Member 4>**            | <netid>    | 
| **<Member 5> (optional)** | <netid>    | 

> Replace placeholders with teammate names/NetIDs.

---

## Problem Statement & Goal

We will build a **hybrid real-time** analytics system that estimates and explains minute-level changes in the **December FOMC decision odds** (e.g., hold vs. cut/hike) by joining:

- **Structured market probabilities** (Polymarket outcome tokens; optional WebSocket for sub-minute updates),
- **Formal baseline probabilities** (CME **FedWatch**; futures-implied),
- **Structured macro features**  (Treasury yields from H.15, inflation from BLS CPI and BEA PCE, and key macroeconomic indicators from FRED, including unemployment, GDP, and the effective federal funds rate),
- **Large-scale news intensity** (GDELT Events/Mentions; 15-minute cadence).

**Outputs:** a calibrated probability series, latency-to-move metrics around macro releases, and attributions of what moved the odds (rates curve shifts, CPI/NFP releases, FRED macro updates, news bursts).

---

## Data Sources, Ownership, Size, and Links

_All raw goes to `/data/bronze` (CSV/JSON/ZIP); cleaned analytics tables go to `/data/silver` (Parquet+Snappy, UTC, partitioned); final features to `/data/gold`._

| #   | Dataset (link)                                                                                                                                                                                                                                                          | What we use it for                                                                 | Cadence                                 |                 Est. size (our 4–8 week window) | Owner (MR profiling & cleaning) |
| --- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- | --------------------------------------- | ----------------------------------------------: | ------------------------------- |
| 1   | **Polymarket – “Fed decision in December”**: Event page `polymarket.com/event/fed-decision-in-december`; Link=>docs.polymarket.com/developers/CLOB/timeseries; APIs: `gamma-api.polymarket.com` (market by slug) & `clob.polymarket.com` (prices-history, book, trades) | Outcome-token **price ≈ probability**; order book (spreads/depth); trades (impact) | Sec–min (poll or WS); backfill via REST | **GBs+** (book snapshots every 10–15s + trades) | **Zephyr**                      |
| 2   | **FRED (Federal Reserve Economic Data)**: [https://fred.stlouisfed.org](https://fred.stlouisfed.org); API: `api.stlouisfed.org/fred/series/observations`                                                                                                                | Core macroeconomic indicators — CPI, unemployment, GDP, Fed funds rate, 10Y yield, and target range upper/lower limits | Daily / Monthly (series-dependent)      |                                         **MBs** | **Yushu LIU**                      |
| 3   | **CME FedWatch – Historical (December meeting)**: `cmegroup.com/.../cme-fedwatch-tool.html` (Historical download)                                                                                                                                                       | **Formal target-range probabilities** (baseline & labels)                          | Daily                                   |                                         **MBs** | <Member 3>                      |
| 4   | **H.15 Treasury Yields (e.g., DGS2, DGS10)**: Fed Data Download Program                                                                                                                                                                                                 | Level/slope factors (rates features)                                               | Daily                                   |                                   **10s of MB** | <Member 4>                      |
| 5   | **BLS CPI (headline/core)**: BLS Public Data API                                                                                                                                                                                                                        | Inflation release values, release timestamps                                       | Monthly (+ schedule)                    |                                         **MBs** | <Member 4>                      |

> We will keep Polymarket as the **large, structured, directly relevant** dataset and use FedWatch/H.15/BLS/BEA as **formal, structured** context. GDELT provides **large-scale** exogenous information flow at a fixed 15-minute cadence.

---

## Schemas & Formats (Silver / analytics-ready)

**All timestamps in UTC. Partition by `dt=YYYY-MM-DD` and `hour=HH` for time-series.**

### Polymarket

- `silver.polymarket_prices_history(token_id STRING, ts TIMESTAMP, price_cents DOUBLE, probability DOUBLE, dt STRING, hour STRING)`
- `silver.polymarket_book_top(token_id STRING, ts TIMESTAMP, best_bid_price DOUBLE, best_bid_size DOUBLE, best_ask_price DOUBLE, best_ask_size DOUBLE, spread DOUBLE, dt STRING, hour STRING)`
- `silver.polymarket_trades(ts TIMESTAMP, token_id STRING, side STRING, price DOUBLE, size DOUBLE, tx_hash STRING, dt STRING, hour STRING)`

### FedWatch

- `silver.fedwatch_probs(meeting STRING, date DATE, target_range_bps STRING, probability DOUBLE)`

### H.15 / CPI / PCE

- `silver.h15(series_id STRING, date DATE, value DOUBLE)`·
- `silver.cpi(series_id STRING, release_ts TIMESTAMP, period DATE, value DOUBLE)`
- `silver.pce(series_id STRING, release_ts TIMESTAMP, period DATE, value DOUBLE)`

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

---

## Usage

### Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Download all data sources (Polymarket + Kalshi + Yahoo Finance + GDELT)
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

- `market_data/polymarket/` - Polymarket trade data
- `market_data/kalshi/` - Kalshi trade data
- `market_data/yfinance/` - Yahoo Finance reference data (Treasury yields, Fed futures)
- `market_data/gdelt/` - GDELT news articles (Federal Reserve related)
- `market_data/fred/` - FRED macroeconomic data (CPI, unemployment, GDP, interest rates, Treasury yields, and other key economic indicators)

### Customizing Data Sources

Edit configuration constants in each data source module:

- **Polymarket**: `data_sources/polymarket.py` - Update `EVENT_SLUG` and `MARKET_LABELS`
- **Kalshi**: `data_sources/kalshi.py` - Update `MARKET_TICKERS`
- **Yahoo Finance**: `data_sources/yfinance.py` - Update `TICKERS`, `DEFAULT_START`, `INTERVAL`
- **GDELT**: `data_sources/gdelt.py` - Update `DEFAULT_QUERIES`, `DEFAULT_PARAMS` (timespan, maxrecords)
- **Fred**: `data_sources/fred.py` - Update SERIES (series IDs to fetch) and .env for FRED_API_KEY; optionally adjust date range or output directory settings.

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
