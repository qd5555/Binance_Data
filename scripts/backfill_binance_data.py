import os
import time
from datetime import datetime, timezone

import psycopg2
import requests
from psycopg2.extras import execute_values

# ============================================================
# BACKFILL BINANCE SPOT DAILY PRICE DATA
#
# Timezone:
#   - All timestamps are stored in UTC.
#
# Table:
#   fact_price_volume
#
# Optional SQL (run manually if you want to reset data)
#
# -- Delete all data
# DELETE FROM fact_price_volume;
#
# -- Delete one symbol
# DELETE FROM fact_price_volume
# WHERE symbol = 'BTCUSDT';
#
# -- Delete data after a date
# DELETE FROM fact_price_volume
# WHERE open_time >= '2025-01-01';
#
# ============================================================

DB_URL = os.environ["DB_CONNECTION_STRING"]

START_DATE = datetime(2023, 1, 1, tzinfo=timezone.utc)

INTERVAL = "1d"

LIMIT = 1000

SLEEP_SECONDS = 0.25


# ------------------------------------------------------------
# Binance API
# ------------------------------------------------------------

def get_all_symbols():
    """
    Return all Binance Spot USDT trading pairs.
    """

    url = "https://data-api.binance.vision/api/v3/exchangeInfo"

    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    data = resp.json()

    symbols = [
        s["symbol"]
        for s in data["symbols"]
        if s["status"] == "TRADING"
        and s["quoteAsset"] == "USDT"
    ]

    symbols.sort()

    return symbols


def get_klines_batch(symbol, start_time_ms):
    """
    Get one batch (max 1000 candles).
    """

    url = (
        "https://data-api.binance.vision/api/v3/klines"
        f"?symbol={symbol}"
        f"&interval={INTERVAL}"
        f"&limit={LIMIT}"
        f"&startTime={start_time_ms}"
    )

    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    return resp.json()


# ------------------------------------------------------------
# Database
# ------------------------------------------------------------

def get_last_timestamp(symbol, cur):
    """
    Resume from latest timestamp in database.
    """

    cur.execute(
        """
        SELECT MAX(open_time)
        FROM fact_price_volume
        WHERE symbol = %s
        """,
        (symbol,),
    )

    result = cur.fetchone()[0]

    if result is None:
        return int(START_DATE.timestamp() * 1000)

    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)

    return int(result.timestamp() * 1000) + 1


def save_batch(symbol, rows, cur):
    """
    Upsert one API batch into PostgreSQL.
    """

    if not rows:
        return

    values = [
        (
            symbol,
            datetime.fromtimestamp(r[0] / 1000, tz=timezone.utc),
            float(r[1]),
            float(r[2]),
            float(r[3]),
            float(r[4]),
            float(r[5]),
        )
        for r in rows
    ]

    execute_values(
        cur,
        """
        INSERT INTO fact_price_volume
        (
            symbol,
            open_time,
            open,
            high,
            low,
            close,
            volume
        )

        VALUES %s

        ON CONFLICT (symbol, open_time)

        DO UPDATE SET

            open   = EXCLUDED.open,
            high   = EXCLUDED.high,
            low    = EXCLUDED.low,
            close  = EXCLUDED.close,
            volume = EXCLUDED.volume
        """,
        values,
    )


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def main():

    print("=" * 60)
    print("BINANCE DAILY BACKFILL")
    print("=" * 60)

    symbols = get_all_symbols()

    print(f"Found {len(symbols)} USDT trading pairs.\n")

    conn = psycopg2.connect(DB_URL)

    cur = conn.cursor()

    success = 0
    failed = []

    try:

        for idx, symbol in enumerate(symbols, start=1):

            print("-" * 60)
            print(f"[{idx}/{len(symbols)}] {symbol}")

            start_ms = get_last_timestamp(symbol, cur)

            resume_time = datetime.fromtimestamp(
                start_ms / 1000,
                tz=timezone.utc
            )

            print(f"Resume from : {resume_time}")

            total_rows = 0
            batch_no = 1

            while True:

                batch = get_klines_batch(symbol, start_ms)

                if not batch:
                    break

                save_batch(symbol, batch, cur)

                conn.commit()

                total_rows += len(batch)

                print(
                    f"Batch {batch_no:<3}"
                    f"+{len(batch):4} rows"
                    f" | Total: {total_rows}"
                )

                if len(batch) < LIMIT:
                    break

                start_ms = batch[-1][0] + 1

                batch_no += 1

                time.sleep(SLEEP_SECONDS)

            print(f"Finished {symbol} ({total_rows} rows)\n")

            success += 1

    except KeyboardInterrupt:

        print("\nInterrupted by user.")

    except Exception as e:

        print(f"\nUnexpected error: {e}")

        raise

    finally:

        cur.close()
        conn.close()

    print("=" * 60)
    print("BACKFILL COMPLETED")
    print("=" * 60)
    print(f"Success : {success}")
    print(f"Failed  : {len(failed)}")

    if failed:
        print(failed)


if __name__ == "__main__":
    main()