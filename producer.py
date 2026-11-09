"""
producer.py — Kafka producer that fetches live stock prices and publishes to Kafka

Polls yfinance at a configurable interval for each symbol, serializes
the tick as JSON, and sends it to the raw prices topic.

Usage:
    python -m src.ingestion.producer
"""

import json
import time
import sys
import os
from datetime import datetime, timezone

import yfinance as yf
from kafka import KafkaProducer
from kafka.errors import KafkaError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_RAW,
    SYMBOLS, FETCH_INTERVAL_SEC,
)


def create_producer() -> KafkaProducer:
    """Create a Kafka producer with JSON serialization."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
    )


def fetch_tick(symbol: str) -> dict | None:
    """Fetch the latest price data for a single symbol."""
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.fast_info

        return {
            "symbol": symbol,
            "price": round(float(info.last_price), 4),
            "volume": int(info.last_volume) if info.last_volume else 0,
            "market_cap": int(info.market_cap) if info.market_cap else None,
            "previous_close": round(float(info.previous_close), 4) if info.previous_close else None,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        print(f"  WARN  failed to fetch {symbol}: {e}")
        return None


def run_producer() -> None:
    producer = create_producer()
    print("=" * 60)
    print("  Stock Price Producer — streaming to Kafka")
    print(f"  Topic: {KAFKA_TOPIC_RAW}")
    print(f"  Symbols: {', '.join(SYMBOLS)}")
    print(f"  Interval: {FETCH_INTERVAL_SEC}s")
    print("=" * 60)

    cycle = 0
    try:
        while True:
            cycle += 1
            print(f"\n[Cycle {cycle}] Fetching prices...")

            for symbol in SYMBOLS:
                tick = fetch_tick(symbol)
                if tick is None:
                    continue

                # Publish to Kafka, keyed by symbol for partition locality
                future = producer.send(
                    KAFKA_TOPIC_RAW,
                    key=symbol,
                    value=tick,
                )
                try:
                    metadata = future.get(timeout=10)
                    print(
                        f"  {symbol}: ${tick['price']:.2f}  "
                        f"→ partition {metadata.partition}, offset {metadata.offset}"
                    )
                except KafkaError as e:
                    print(f"  ERROR sending {symbol}: {e}")

            producer.flush()
            time.sleep(FETCH_INTERVAL_SEC)

    except KeyboardInterrupt:
        print("\nShutting down producer...")
    finally:
        producer.close()


if __name__ == "__main__":
    run_producer()
