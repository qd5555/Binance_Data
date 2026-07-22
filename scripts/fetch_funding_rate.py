import os
import time
from datetime import datetime, timezone

import psycopg2
import requests
from psycopg2.extras import execute_values

DB_URL = os.environ["DB_CONNECTION_STRING"]

LIMIT = 1000

SLEEP_SECONDS = 0.2

EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"

FUNDING_URL = "https://fapi.binance.com/fapi/v1/fundingRate"


# ----------------------------------------------------------
# Futures symbols
# ----------------------------------------------------------

def get_symbols():

    r = requests.get(EXCHANGE_INFO_URL, timeout=30)

    r.raise_for_status()

    data = r.json()

    symbols = [

        s["symbol"]

        for s in data["symbols"]

        if s["status"] == "TRADING"

        and s["quoteAsset"] == "USDT"

    ]

    symbols.sort()

    return symbols


# ----------------------------------------------------------
# Database
# ----------------------------------------------------------

def get_last_timestamp(symbol, cur):

    cur.execute(

        """

        SELECT MAX(funding_time)

        FROM fact_funding_rate

        WHERE symbol=%s

        """,

        (symbol,),

    )

    result = cur.fetchone()[0]

    if result is None:

        return None

    if result.tzinfo is None:

        result = result.replace(tzinfo=timezone.utc)

    return int(result.timestamp() * 1000) + 1


# ----------------------------------------------------------
# Binance API
# ----------------------------------------------------------

def fetch_batch(symbol, start_ms=None):

    params = {

        "symbol": symbol,

        "limit": LIMIT,

    }

    if start_ms is not None:

        params["startTime"] = start_ms

    r = requests.get(

        FUNDING_URL,

        params=params,

        timeout=30,

    )

    r.raise_for_status()

    return r.json()


# ----------------------------------------------------------
# Save
# ----------------------------------------------------------

def save(rows, cur):

    if not rows:

        return

    values = []

    for r in rows:

        values.append(

            (

                r["symbol"],

                datetime.fromtimestamp(

                    int(r["fundingTime"]) / 1000,

                    tz=timezone.utc,

                ),

                float(r["fundingRate"]),

                float(r["markPrice"]),

            )

        )

    execute_values(

        cur,

        """

        INSERT INTO fact_funding_rate

        (

            symbol,

            funding_time,

            funding_rate,

            mark_price

        )

        VALUES %s

        ON CONFLICT(symbol,funding_time)

        DO UPDATE SET

            funding_rate=EXCLUDED.funding_rate,

            mark_price=EXCLUDED.mark_price

        """,

        values,

    )


# ----------------------------------------------------------
# Main
# ----------------------------------------------------------

def main():

    print("=" * 60)

    print("FETCH BINANCE FUNDING RATE")

    print("=" * 60)

    conn = psycopg2.connect(DB_URL)

    cur = conn.cursor()

    symbols = get_symbols()

    success = 0

    total_rows = 0

    try:

        for idx, symbol in enumerate(symbols, start=1):

            print(f"[{idx}/{len(symbols)}] {symbol}")

            start_ms = get_last_timestamp(symbol, cur)

            while True:

                rows = fetch_batch(symbol, start_ms)

                if not rows:

                    break

                save(rows, cur)

                conn.commit()

                total_rows += len(rows)

                print(f"    +{len(rows)} rows")

                if len(rows) < LIMIT:

                    break

                start_ms = int(rows[-1]["fundingTime"]) + 1

                time.sleep(SLEEP_SECONDS)

            success += 1

    finally:

        cur.close()

        conn.close()

    print()

    print("=" * 60)

    print("DONE")

    print("=" * 60)

    print(f"Symbols : {success}")

    print(f"Rows    : {total_rows}")


if __name__ == "__main__":

    main()