"""
settings.py — Central configuration for the real-time stock pipeline
"""

# ─── Kafka ────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC_RAW        = "stock-prices-raw"
KAFKA_TOPIC_ENRICHED   = "stock-prices-enriched"
KAFKA_CONSUMER_GROUP   = "stock-pipeline-consumer"

# ─── Stock Symbols ────────────────────────────────────────────
# Add/remove tickers here — pipeline picks them up automatically
SYMBOLS = [
    "AAPL", "GOOGL", "MSFT", "AMZN", "TSLA",
    "META", "NVDA", "JPM", "V", "WMT",
]

# ─── Data Source ──────────────────────────────────────────────
# yfinance for free real-time-ish data (15-min delay on free tier)
FETCH_INTERVAL_SEC = 10   # seconds between API polls

# ─── PostgreSQL ───────────────────────────────────────────────
PG_HOST     = "localhost"
PG_PORT     = 5432
PG_DB       = "stockpipeline"
PG_USER     = "postgres"
PG_PASSWORD = "postgres"

# ─── Processing ───────────────────────────────────────────────
ROLLING_WINDOW_SHORT  = 20    # periods for short moving average
ROLLING_WINDOW_LONG   = 50    # periods for long moving average
VOLATILITY_WINDOW     = 20    # periods for rolling volatility
VWAP_WINDOW           = 30    # periods for VWAP calculation
