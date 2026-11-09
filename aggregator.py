"""
aggregator.py — Periodic job that rolls up enriched ticks into daily summaries

Runs once (or on a cron) to compute OHLC, average volume, VWAP,
and daily returns from the enriched_ticks table.

Usage:
    python -m src.storage.aggregator
"""

import sys
import os
from datetime import date

import psycopg2

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from config.settings import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD


UPSERT_DAILY = """
INSERT INTO stock.daily_summary
    (symbol, trade_date, open_price, close_price, high_price, low_price,
     avg_volume, total_ticks, avg_vwap, daily_return_pct)
SELECT
    symbol,
    DATE(timestamp)                           AS trade_date,
    (ARRAY_AGG(price ORDER BY timestamp ASC))[1]  AS open_price,
    (ARRAY_AGG(price ORDER BY timestamp DESC))[1] AS close_price,
    MAX(price)                                AS high_price,
    MIN(price)                                AS low_price,
    AVG(volume)::BIGINT                       AS avg_volume,
    COUNT(*)                                  AS total_ticks,
    AVG(vwap)                                 AS avg_vwap,
    CASE
        WHEN (ARRAY_AGG(price ORDER BY timestamp ASC))[1] > 0
        THEN ROUND(
            ((ARRAY_AGG(price ORDER BY timestamp DESC))[1]
             - (ARRAY_AGG(price ORDER BY timestamp ASC))[1])
            / (ARRAY_AGG(price ORDER BY timestamp ASC))[1] * 100, 6
        )
        ELSE NULL
    END                                       AS daily_return_pct
FROM stock.enriched_ticks
WHERE DATE(timestamp) = %s
GROUP BY symbol, DATE(timestamp)
ON CONFLICT (symbol, trade_date)
DO UPDATE SET
    close_price     = EXCLUDED.close_price,
    high_price      = EXCLUDED.high_price,
    low_price       = EXCLUDED.low_price,
    avg_volume      = EXCLUDED.avg_volume,
    total_ticks     = EXCLUDED.total_ticks,
    avg_vwap        = EXCLUDED.avg_vwap,
    daily_return_pct = EXCLUDED.daily_return_pct,
    updated_at      = NOW();
"""


def run_aggregation(target_date: date | None = None) -> None:
    if target_date is None:
        target_date = date.today()

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
    )
    conn.autocommit = True

    print(f"Aggregating daily summary for {target_date}...")

    with conn.cursor() as cur:
        cur.execute(UPSERT_DAILY, (target_date,))
        print(f"  Upserted {cur.rowcount} symbol summaries")

        # Show results
        cur.execute(
            "SELECT symbol, open_price, close_price, high_price, low_price, "
            "total_ticks, daily_return_pct FROM stock.daily_summary "
            "WHERE trade_date = %s ORDER BY symbol",
            (target_date,),
        )
        rows = cur.fetchall()
        print(f"\n  {'Symbol':<8} {'Open':>10} {'Close':>10} {'High':>10} {'Low':>10} {'Ticks':>6} {'Return%':>10}")
        print("  " + "-" * 72)
        for r in rows:
            ret = f"{r[6]:.3f}%" if r[6] else "N/A"
            print(f"  {r[0]:<8} {r[1]:>10.2f} {r[2]:>10.2f} {r[3]:>10.2f} {r[4]:>10.2f} {r[5]:>6} {ret:>10}")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    run_aggregation()
