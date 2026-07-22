import os
from datetime import datetime, timezone

import psycopg2
import requests
from psycopg2.extras import execute_values

# ============================================================
# FETCH BINANCE SYMBOL METADATA
#
# Table:
#     dim_symbol
#
# Timezone:
#     UTC
#
# ============================================================

DB_URL = os.environ["DB_CONNECTION_STRING"]

EXCHANGE_INFO_URL = "https://data-api.binance.vision/api/v3/exchangeInfo"


def fetch_symbols():
    """
    Fetch Binance Spot USDT trading pairs.
    """

    resp = requests.get(EXCHANGE_INFO_URL, timeout=30)
    resp.raise_for_status()

    data = resp.json()

    rows = []

    now = datetime.now(timezone.utc)

    for s in data["symbols"]:

        if s["quoteAsset"] != "USDT":
            continue

        rows.append(
            (
                s["symbol"],
                s["baseAsset"],
                s["quoteAsset"],
                s["status"],
                s["isSpotTradingAllowed"],
                s["isMarginTradingAllowed"],
                now,
                now,
            )
        )

    rows.sort(key=lambda x: x[0])

    return rows


def save_metadata(rows):

    conn = psycopg2.connect(DB_URL)

    cur = conn.cursor()

    execute_values(
        cur,
        """
        INSERT INTO dim_symbol
        (
            symbol,
            base_asset,
            quote_asset,
            status,
            is_spot_trading_allowed,
            is_margin_trading_allowed,
            created_at,
            updated_at
        )

        VALUES %s

        ON CONFLICT(symbol)

        DO UPDATE SET

            base_asset = EXCLUDED.base_asset,
            quote_asset = EXCLUDED.quote_asset,
            status = EXCLUDED.status,
            is_spot_trading_allowed = EXCLUDED.is_spot_trading_allowed,
            is_margin_trading_allowed = EXCLUDED.is_margin_trading_allowed,
            updated_at = EXCLUDED.updated_at
        """,
        rows,
    )

    conn.commit()

    cur.close()

    conn.close()


def main():

    print("=" * 60)
    print("FETCH BINANCE SYMBOL METADATA")
    print("=" * 60)

    rows = fetch_symbols()

    print(f"Fetched {len(rows)} symbols")

    save_metadata(rows)

    print("Metadata updated successfully.")


if __name__ == "__main__":

    main()