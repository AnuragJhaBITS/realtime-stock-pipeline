-- ═══════════════════════════════════════════════════════════════
-- PostgreSQL schema for the real-time stock pipeline
-- Auto-executed on container startup via docker-compose mount
-- ═══════════════════════════════════════════════════════════════

CREATE SCHEMA IF NOT EXISTS stock;
SET search_path TO stock;

-- ─── Raw price ticks from Kafka ──────────────────────────────
CREATE TABLE IF NOT EXISTS raw_ticks (
    id              BIGSERIAL PRIMARY KEY,
    symbol          VARCHAR(10)    NOT NULL,
    price           DECIMAL(12,4)  NOT NULL,
    volume          BIGINT,
    bid             DECIMAL(12,4),
    ask             DECIMAL(12,4),
    market_cap      BIGINT,
    timestamp       TIMESTAMPTZ    NOT NULL,
    ingested_at     TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ticks_symbol_ts
    ON raw_ticks (symbol, timestamp DESC);

-- ─── Enriched data with computed indicators ──────────────────
CREATE TABLE IF NOT EXISTS enriched_ticks (
    id              BIGSERIAL PRIMARY KEY,
    symbol          VARCHAR(10)    NOT NULL,
    price           DECIMAL(12,4)  NOT NULL,
    volume          BIGINT,
    timestamp       TIMESTAMPTZ    NOT NULL,

    -- Moving averages
    sma_20          DECIMAL(12,4),
    sma_50          DECIMAL(12,4),
    ema_20          DECIMAL(12,4),

    -- Volatility
    rolling_std_20  DECIMAL(12,6),
    pct_change      DECIMAL(10,6),

    -- VWAP
    vwap            DECIMAL(12,4),

    -- Signals
    sma_cross_signal VARCHAR(10),   -- 'BULLISH', 'BEARISH', or 'NEUTRAL'
    price_vs_vwap    VARCHAR(10),   -- 'ABOVE' or 'BELOW'

    computed_at     TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_enriched_symbol_ts
    ON enriched_ticks (symbol, timestamp DESC);

-- ─── Alerts log ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS alerts (
    id              BIGSERIAL PRIMARY KEY,
    symbol          VARCHAR(10)    NOT NULL,
    alert_type      VARCHAR(30)    NOT NULL,
    message         TEXT           NOT NULL,
    price           DECIMAL(12,4),
    triggered_at    TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

-- ─── Aggregated daily summary (for dashboard) ────────────────
CREATE TABLE IF NOT EXISTS daily_summary (
    symbol          VARCHAR(10)    NOT NULL,
    trade_date      DATE           NOT NULL,
    open_price      DECIMAL(12,4),
    close_price     DECIMAL(12,4),
    high_price      DECIMAL(12,4),
    low_price       DECIMAL(12,4),
    avg_volume      BIGINT,
    total_ticks     INT,
    avg_vwap        DECIMAL(12,4),
    daily_return_pct DECIMAL(10,6),
    updated_at      TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, trade_date)
);
