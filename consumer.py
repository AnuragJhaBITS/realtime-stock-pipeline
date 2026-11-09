"""
consumer.py — Kafka consumer that reads raw ticks, computes indicators,
              and writes enriched data to PostgreSQL

Reads from the raw prices topic, maintains a rolling window per symbol
in memory, computes technical indicators, generates alerts on crossover
signals, and persists everything to PostgreSQL.

Usage:
    python -m src.processing.consumer
"""

import json
import sys
import os
from collections import defaultdict, deque
from datetime import datetime, timezone
from decimal import Decimal

import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaConsumer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_RAW, KAFKA_CONSUMER_GROUP,
    PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD,
    ROLLING_WINDOW_SHORT, ROLLING_WINDOW_LONG,
    VOLATILITY_WINDOW, VWAP_WINDOW,
)


# ─────────────────────── In-memory rolling state ──────────────

class SymbolState:
    """Maintains a rolling window of price/volume data for one symbol."""

    def __init__(self, max_window: int = 100):
        self.prices: deque[float] = deque(maxlen=max_window)
        self.volumes: deque[int] = deque(maxlen=max_window)
        self.pv_products: deque[float] = deque(maxlen=max_window)  # price * volume
        self.ema_short: float | None = None
        self.prev_sma_cross: str | None = None

    def add(self, price: float, volume: int) -> None:
        self.prices.append(price)
        self.volumes.append(volume)
        self.pv_products.append(price * volume)

    def sma(self, window: int) -> float | None:
        if len(self.prices) < window:
            return None
        return sum(list(self.prices)[-window:]) / window

    def ema(self, window: int) -> float | None:
        if len(self.prices) < window:
            return None
        multiplier = 2 / (window + 1)
        if self.ema_short is None:
            self.ema_short = self.sma(window)
        else:
            self.ema_short = (self.prices[-1] - self.ema_short) * multiplier + self.ema_short
        return self.ema_short

    def rolling_std(self, window: int) -> float | None:
        if len(self.prices) < window:
            return None
        recent = list(self.prices)[-window:]
        mean = sum(recent) / len(recent)
        variance = sum((x - mean) ** 2 for x in recent) / len(recent)
        return variance ** 0.5

    def pct_change(self) -> float | None:
        if len(self.prices) < 2:
            return None
        prev = self.prices[-2]
        if prev == 0:
            return None
        return (self.prices[-1] - prev) / prev

    def vwap(self, window: int) -> float | None:
        if len(self.prices) < window:
            return None
        recent_pv = list(self.pv_products)[-window:]
        recent_v = list(self.volumes)[-window:]
        total_vol = sum(recent_v)
        if total_vol == 0:
            return None
        return sum(recent_pv) / total_vol

    def sma_cross_signal(self) -> str:
        short = self.sma(ROLLING_WINDOW_SHORT)
        long_ = self.sma(ROLLING_WINDOW_LONG)
        if short is None or long_ is None:
            return "NEUTRAL"
        if short > long_:
            return "BULLISH"
        elif short < long_:
            return "BEARISH"
        return "NEUTRAL"


# ─────────────────────── Database helpers ─────────────────────

def get_pg_connection():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
    )


def insert_raw_tick(cur, tick: dict) -> None:
    cur.execute(
        """
        INSERT INTO stock.raw_ticks (symbol, price, volume, market_cap, timestamp)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (tick["symbol"], tick["price"], tick["volume"],
         tick.get("market_cap"), tick["timestamp"]),
    )


def insert_enriched_tick(cur, row: dict) -> None:
    cur.execute(
        """
        INSERT INTO stock.enriched_ticks
            (symbol, price, volume, timestamp,
             sma_20, sma_50, ema_20, rolling_std_20, pct_change,
             vwap, sma_cross_signal, price_vs_vwap)
        VALUES (%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s)
        """,
        (
            row["symbol"], row["price"], row["volume"], row["timestamp"],
            row["sma_20"], row["sma_50"], row["ema_20"],
            row["rolling_std_20"], row["pct_change"],
            row["vwap"], row["sma_cross_signal"], row["price_vs_vwap"],
        ),
    )


def insert_alert(cur, symbol: str, alert_type: str, message: str, price: float) -> None:
    cur.execute(
        """
        INSERT INTO stock.alerts (symbol, alert_type, message, price)
        VALUES (%s, %s, %s, %s)
        """,
        (symbol, alert_type, message, price),
    )


# ─────────────────────── Main consumer loop ───────────────────

def run_consumer() -> None:
    consumer = KafkaConsumer(
        KAFKA_TOPIC_RAW,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_CONSUMER_GROUP,
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )

    conn = get_pg_connection()
    conn.autocommit = True

    # Per-symbol rolling state in memory
    states: dict[str, SymbolState] = defaultdict(lambda: SymbolState(max_window=200))

    print("=" * 60)
    print("  Stock Pipeline Consumer — processing ticks")
    print(f"  Topic: {KAFKA_TOPIC_RAW}")
    print(f"  Consumer group: {KAFKA_CONSUMER_GROUP}")
    print("=" * 60)

    processed = 0
    alerts_fired = 0

    try:
        for message in consumer:
            tick = message.value
            symbol = tick["symbol"]
            price = float(tick["price"])
            volume = int(tick.get("volume", 0))

            # Update rolling state
            state = states[symbol]
            state.add(price, volume)

            # Compute indicators
            sma_20 = state.sma(ROLLING_WINDOW_SHORT)
            sma_50 = state.sma(ROLLING_WINDOW_LONG)
            ema_20 = state.ema(ROLLING_WINDOW_SHORT)
            std_20 = state.rolling_std(VOLATILITY_WINDOW)
            pct = state.pct_change()
            vwap = state.vwap(VWAP_WINDOW)
            cross = state.sma_cross_signal()
            vs_vwap = "ABOVE" if (vwap and price > vwap) else "BELOW" if vwap else None

            enriched = {
                "symbol": symbol,
                "price": price,
                "volume": volume,
                "timestamp": tick["timestamp"],
                "sma_20": round(sma_20, 4) if sma_20 else None,
                "sma_50": round(sma_50, 4) if sma_50 else None,
                "ema_20": round(ema_20, 4) if ema_20 else None,
                "rolling_std_20": round(std_20, 6) if std_20 else None,
                "pct_change": round(pct, 6) if pct else None,
                "vwap": round(vwap, 4) if vwap else None,
                "sma_cross_signal": cross,
                "price_vs_vwap": vs_vwap,
            }

            # Write to PostgreSQL
            with conn.cursor() as cur:
                insert_raw_tick(cur, tick)
                insert_enriched_tick(cur, enriched)

                # Check for SMA crossover alerts
                prev_cross = state.prev_sma_cross
                if prev_cross and prev_cross != cross and cross != "NEUTRAL":
                    alert_msg = (
                        f"{symbol} SMA crossover: {prev_cross} → {cross} "
                        f"(SMA20={sma_20:.2f}, SMA50={sma_50:.2f}, price=${price:.2f})"
                    )
                    insert_alert(cur, symbol, "SMA_CROSSOVER", alert_msg, price)
                    print(f"  ALERT  {alert_msg}")
                    alerts_fired += 1

                # Check for large price moves (>2% in one tick)
                if pct and abs(pct) > 0.02:
                    alert_msg = (
                        f"{symbol} large move: {pct*100:+.2f}% "
                        f"(price=${price:.2f})"
                    )
                    insert_alert(cur, symbol, "LARGE_MOVE", alert_msg, price)
                    print(f"  ALERT  {alert_msg}")
                    alerts_fired += 1

            state.prev_sma_cross = cross
            processed += 1

            if processed % 10 == 0:
                print(f"  Processed {processed} ticks | {alerts_fired} alerts")

    except KeyboardInterrupt:
        print(f"\nShutting down — {processed} ticks processed, {alerts_fired} alerts fired")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    run_consumer()
