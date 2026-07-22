-- ============================================================
-- Binance Crypto Analytics Database
-- PostgreSQL / Supabase
--
-- Timezone:
--     All timestamps are stored in UTC.
--
-- Author:
--     Quang Dung
-- ============================================================

-- ============================================================
-- Optional: Drop existing tables
-- Uncomment ONLY if you want to rebuild the database
-- ============================================================

-- DROP TABLE IF EXISTS fact_price_volume;
-- DROP TABLE IF EXISTS fact_funding_rate;
-- DROP TABLE IF EXISTS dim_symbol;
-- DROP TABLE IF EXISTS dim_coin;


-- ============================================================
-- DIMENSION TABLE : SYMBOL
-- Source:
-- Binance ExchangeInfo API
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_symbol (

    symbol                      VARCHAR(20) PRIMARY KEY,

    base_asset                  VARCHAR(20) NOT NULL,

    quote_asset                 VARCHAR(20) NOT NULL,

    status                      VARCHAR(20),

    is_spot_trading_allowed     BOOLEAN,

    is_margin_trading_allowed   BOOLEAN,

    created_at                  TIMESTAMP NOT NULL,

    updated_at                  TIMESTAMP NOT NULL

);



-- ============================================================
-- DIMENSION TABLE : COIN
-- Source:
-- CoinGecko
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_coin (

    symbol                      VARCHAR(20) PRIMARY KEY,

    coin_id                     VARCHAR(100),

    coin_name                   VARCHAR(100),

    market_cap                  NUMERIC(30,2),

    market_cap_rank             INTEGER,

    circulating_supply          NUMERIC(38,8),

    total_supply                NUMERIC(38,8),

    max_supply                  NUMERIC(38,8),

    fully_diluted_valuation     NUMERIC(30,2),

    current_price               NUMERIC(30,10),

    last_updated                TIMESTAMP

);



-- ============================================================
-- FACT TABLE : DAILY OHLCV
-- Source:
-- Binance Spot Klines
--
-- Grain:
-- 1 row = 1 symbol × 1 candle
--
-- Timezone:
-- UTC
-- ============================================================

CREATE TABLE IF NOT EXISTS fact_price_volume (

    symbol          VARCHAR(20) NOT NULL,

    open_time       TIMESTAMP NOT NULL,

    open            NUMERIC(30,10),

    high            NUMERIC(30,10),

    low             NUMERIC(30,10),

    close           NUMERIC(30,10),

    volume          NUMERIC(38,10),

    PRIMARY KEY(symbol, open_time)

);



-- ============================================================
-- FACT TABLE : FUNDING RATE
-- Source:
-- Binance Futures API
--
-- Grain:
-- 1 row = 1 funding event
-- ============================================================

CREATE TABLE IF NOT EXISTS fact_funding_rate (

    symbol              VARCHAR(20) NOT NULL,

    funding_time        TIMESTAMP NOT NULL,

    funding_rate        NUMERIC(18,10),

    mark_price          NUMERIC(30,10),

    PRIMARY KEY(symbol, funding_time)

);



-- ============================================================
-- Indexes
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_price_symbol
ON fact_price_volume(symbol);

CREATE INDEX IF NOT EXISTS idx_price_time
ON fact_price_volume(open_time);

CREATE INDEX IF NOT EXISTS idx_funding_symbol
ON fact_funding_rate(symbol);

CREATE INDEX IF NOT EXISTS idx_funding_time
ON fact_funding_rate(funding_time);

CREATE INDEX IF NOT EXISTS idx_marketcap_rank
ON dim_coin(market_cap_rank);



-- ============================================================
-- Table Comments
-- ============================================================

COMMENT ON TABLE fact_price_volume IS
'Daily OHLCV data from Binance Spot API (UTC).';

COMMENT ON TABLE fact_funding_rate IS
'Funding rate history from Binance Futures API (UTC).';

COMMENT ON TABLE dim_symbol IS
'Trading symbol metadata from Binance ExchangeInfo API.';

COMMENT ON TABLE dim_coin IS
'Coin market capitalization and supply information from CoinGecko.';