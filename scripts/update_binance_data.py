import os
import time
from datetime import timezone

import psycopg2
import requests
from psycopg2.extras import execute_values

# ============================================================
# UPDATE BINANCE DAILY PRICE DATA
#
# Purpose:
#     Incrementally update fact_price_volume.
#
# Timezone:
#     UTC
#
# ============================================================

DB_URL = os.environ["DB_CONNECTION_STRING"]

INTERVAL = "1d"

LIMIT = 1000

SLEEP_SECONDS = 0.20


# ------------------------------------------------------------
# Binance API
# ------------------------------------------------------------

def get_all_symbols():

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


def get_new_klines(symbol, start_ms):

    url = (
        "https://data-api.binance.vision/api/v3/klines"
        f"?symbol={symbol}"
        f"&interval={INTERVAL}"
        f"&limit={LIMIT}"
        f"&startTime={start_ms}"
    )

    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    return resp.json()


# ------------------------------------------------------------
# Database
# ------------------------------------------------------------

def get_last_timestamp(symbol, cur):

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
        return None

    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)

    return int(result.timestamp() * 1000) + 1


def save_batch(symbol, rows, cur):

    if not rows:
        return

    values = [
        (
            symbol,
            rows[i][0] / 1000,
            float(rows[i][1]),
            float(rows[i][2]),
            float(rows[i][3]),
            float(rows[i][4]),
            float(rows[i][5]),
        )
        for i in range(len(rows))
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

            open=EXCLUDED.open,
            high=EXCLUDED.high,
            low=EXCLUDED.low,
            close=EXCLUDED.close,
            volume=EXCLUDED.volume
        """,
        [
            (
                v[0],
                psycopg2.TimestampFromTicks(v[1]),
                v[2],
                v[3],
                v[4],
                v[5],
                v[6],
            )
            for v in values
        ],
    )


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def main():

    print("=" * 60)
    print("BINANCE DAILY UPDATE")
    print("=" * 60)

    symbols = get_all_symbols()

    conn = psycopg2.connect(DB_URL)

    cur = conn.cursor()

    updated_symbols = 0
    inserted_rows = 0
    failed = []

    try:

        for idx, symbol in enumerate(symbols, start=1):

            print(f"[{idx}/{len(symbols)}] {symbol}")

            start_ms = get_last_timestamp(symbol, cur)

            if start_ms is None:
                print("    No historical data. Skip.")
                continue

            while True:

                rows = get_new_klines(symbol, start_ms)

                if not rows:
                    break

                save_batch(symbol, rows, cur)

                conn.commit()

                inserted_rows += len(rows)

                print(f"    +{len(rows)} rows")

                if len(rows) < LIMIT:
                    break

                start_ms = rows[-1][0] + 1

                time.sleep(SLEEP_SECONDS)

            updated_symbols += 1

    except KeyboardInterrupt:

        print("\nInterrupted.")

    except Exception as e:

        print(e)
        raise

    finally:

        cur.close()
        conn.close()

    print("\n")

    print("=" * 60)
    print("UPDATE COMPLETED")
    print("=" * 60)

    print(f"Updated symbols : {updated_symbols}")

    print(f"Inserted rows   : {inserted_rows}")

    print(f"Failed symbols  : {len(failed)}")


if __name__ == "__main__":

    main()