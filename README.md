# Real-Time Stock Market Data Pipeline

A streaming data pipeline that ingests live stock prices via the Yahoo Finance API, streams them through Apache Kafka, computes real-time technical indicators, persists enriched data to PostgreSQL, and serves a live analytics dashboard.

## Architecture

```
┌────────────┐     ┌─────────────┐     ┌──────────────────┐     ┌────────────┐     ┌────────────┐
│  Yahoo     │────▶│   Kafka     │────▶│   Consumer       │────▶│ PostgreSQL │────▶│ Streamlit  │
│  Finance   │     │  Producer   │     │  (Enrichment)    │     │            │     │ Dashboard  │
│  API       │     │             │     │                  │     │            │     │            │
└────────────┘     └─────────────┘     └──────────────────┘     └────────────┘     └────────────┘
  10 symbols         Raw ticks           Indicators:              4 tables:          Live charts,
  polled every       keyed by            • SMA 20/50              • raw_ticks        KPIs, alerts
  10 seconds         symbol              • EMA 20                 • enriched_ticks
                                         • VWAP                   • alerts
                                         • Volatility             • daily_summary
                                         • Crossover alerts
```

## Pipeline Components

### 1. Producer (`src/ingestion/producer.py`)
- Polls Yahoo Finance API for 10 stock symbols every 10 seconds
- Serializes tick data (price, volume, market cap) as JSON
- Publishes to Kafka topic keyed by symbol (ensures partition locality)
- Handles API failures gracefully with per-symbol retry

### 2. Consumer (`src/processing/consumer.py`)
- Reads raw ticks from Kafka in real time
- Maintains per-symbol rolling windows in memory (up to 200 ticks)
- Computes technical indicators on each tick:
  - **SMA 20 / SMA 50** — simple moving averages
  - **EMA 20** — exponential moving average
  - **VWAP** — volume-weighted average price (30-period)
  - **Rolling volatility** — 20-period standard deviation
  - **Percentage change** — tick-over-tick
- Generates alerts on:
  - SMA crossover signals (bullish/bearish)
  - Large price moves (>2% in one tick)
- Writes raw ticks, enriched ticks, and alerts to PostgreSQL

### 3. Aggregator (`src/storage/aggregator.py`)
- Batch job that rolls up enriched ticks into daily OHLC summaries
- Computes open, close, high, low, average volume, VWAP, and daily returns
- Upserts to `daily_summary` table (idempotent — safe to re-run)

### 4. Dashboard (`src/dashboard/app.py`)
- Real-time Streamlit dashboard with 5-second auto-refresh
- KPI tiles: price, SMAs, EMA, VWAP, crossover signal
- Interactive Plotly chart: price + moving averages + volume
- Alert feed: latest crossover and large-move alerts
- Cross-symbol comparison table

## Database Schema

```
stock.raw_ticks          — Raw price ticks as received from Kafka
stock.enriched_ticks     — Ticks with computed indicators and signals
stock.alerts             — SMA crossover and large-move alerts
stock.daily_summary      — Aggregated daily OHLC + returns
```

## Project Structure

```
realtime-stock-pipeline/
├── README.md
├── requirements.txt
├── config/
│   └── settings.py              # Kafka, PostgreSQL, symbol config
├── docker/
│   └── docker-compose.yml       # Kafka + Zookeeper + PostgreSQL
├── sql/
│   └── init.sql                 # Database schema (auto-runs on startup)
└── src/
    ├── ingestion/
    │   └── producer.py          # Kafka producer — fetches live prices
    ├── processing/
    │   └── consumer.py          # Kafka consumer — computes indicators
    ├── storage/
    │   └── aggregator.py        # Daily OHLC rollup job
    └── dashboard/
        └── app.py               # Streamlit real-time dashboard
```

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.10+

### 1. Start infrastructure
```bash
cd docker
docker-compose up -d
```
This starts Kafka (with Zookeeper) and PostgreSQL. The database schema is auto-created on first run.

### 2. Install Python dependencies
```bash
pip install -r requirements.txt
```

### 3. Start the producer (Terminal 1)
```bash
python -m src.ingestion.producer
```

### 4. Start the consumer (Terminal 2)
```bash
python -m src.processing.consumer
```

### 5. Launch the dashboard (Terminal 3)
```bash
streamlit run src/dashboard/app.py
```

### 6. Run daily aggregation (optional)
```bash
python -m src.storage.aggregator
```

## Tech Stack

| Component     | Technology                          |
|---------------|-------------------------------------|
| Data Source    | Yahoo Finance API (yfinance)        |
| Message Queue | Apache Kafka                        |
| Processing    | Python (in-memory rolling windows)  |
| Storage       | PostgreSQL                          |
| Dashboard     | Streamlit + Plotly                  |
| Infra         | Docker Compose                      |

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Kafka over direct DB writes | Decouples ingestion from processing; allows replay and multiple consumers |
| Symbol-keyed messages | Ensures all ticks for one symbol land on the same partition — ordering guaranteed |
| In-memory rolling windows | Sub-millisecond indicator computation; no repeated DB reads per tick |
| PostgreSQL over time-series DB | Familiar, production-grade, supports both OLTP inserts and OLAP aggregations |
| Separate raw + enriched tables | Preserves source data; enriched layer can be rebuilt from raw if logic changes |
| Upsert-based daily aggregation | Idempotent — safe to re-run without duplicates |
