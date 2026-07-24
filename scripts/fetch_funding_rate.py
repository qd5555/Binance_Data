import os
import time
import requests
import psycopg2

from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from psycopg2.extras import execute_values

# ============================================================
# CONFIG
# ============================================================

DB_URL = os.environ["DB_CONNECTION_STRING"]

BASE_URL = os.environ.get("BINANCE_PROXY_URL", "https://fapi.binance.com")
EXCHANGE_INFO_URL = f"{BASE_URL}/fapi/v1/exchangeInfo"
FUNDING_URL = f"{BASE_URL}/fapi/v1/fundingRate"

MAX_WORKERS = 20

REQUEST_TIMEOUT = 20

MAX_RETRY = 3

RETRY_SLEEP = 2

LIMIT = 1000


# ============================================================
# DATABASE
# ============================================================

def get_symbols(cur):

    cur.execute("""
        SELECT symbol
        FROM dim_symbol
        WHERE status='TRADING'
        ORDER BY symbol
    """)

    return [r[0] for r in cur.fetchall()]


def get_last_funding(cur):

    cur.execute("""
        SELECT
            symbol,
            MAX(funding_time)
        FROM fact_funding_rate
        GROUP BY symbol
    """)

    result = {}

    for symbol, ts in cur.fetchall():

        if ts is None:

            result[symbol] = None

        else:

            result[symbol] = int(ts.timestamp() * 1000) + 1

    return result


# ============================================================
# HTTP
# ============================================================

def request_with_retry(url, params):

    for attempt in range(MAX_RETRY):

        try:

            r = requests.get(
                url,
                params=params,
                timeout=REQUEST_TIMEOUT
            )

            r.raise_for_status()

            return r.json()

        except Exception:

            if attempt == MAX_RETRY - 1:
                raise

            time.sleep(RETRY_SLEEP)

    return []


# ============================================================
# DOWNLOAD ONE SYMBOL
# ============================================================

def fetch_symbol(symbol, last_timestamp):

    params = {

        "symbol": symbol,

        "limit": LIMIT

    }

    if last_timestamp is not None:

        params["startTime"] = last_timestamp

    try:

        rows = request_with_retry(
            FUNDING_URL,
            params
        )

        values = []

        for r in rows:

            values.append(

                (

                    symbol,

                    datetime.fromtimestamp(

                        int(r["fundingTime"]) / 1000,

                        tz=timezone.utc

                    ),

                    float(r["fundingRate"]),

                    float(r["markPrice"])

                )

            )

        return values

    except Exception as e:

        print(f"{symbol} FAILED : {e}")

        return []
# ============================================================
# SAVE
# ============================================================

def save_all_rows(cur, conn, rows):

    if not rows:
        return 0

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

        ON CONFLICT(symbol, funding_time)

        DO UPDATE SET

            funding_rate = EXCLUDED.funding_rate,
            mark_price   = EXCLUDED.mark_price

        """,

        rows,

        page_size=500

    )

    conn.commit()

    return len(rows)


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 60)
    print("BINANCE FUNDING RATE UPDATE")
    print("=" * 60)

    conn = psycopg2.connect(DB_URL)

    cur = conn.cursor()

    try:

        # ----------------------------------------------------
        # Load symbols
        # ----------------------------------------------------

        symbols = get_symbols(cur)

        print(f"Trading symbols : {len(symbols)}")

        # ----------------------------------------------------
        # Last funding map
        # ----------------------------------------------------

        last_funding = get_last_funding(cur)

        print("Last funding map loaded.")

        # ----------------------------------------------------
        # Download concurrently
        # ----------------------------------------------------

        print("\nDownloading funding rates...\n")

        all_rows = []

        completed = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

            futures = {

                executor.submit(
                    fetch_symbol,
                    symbol,
                    last_funding.get(symbol)
                ): symbol

                for symbol in symbols

            }

            for future in as_completed(futures):

                symbol = futures[future]

                try:

                    rows = future.result()

                    if rows:

                        all_rows.extend(rows)

                except Exception as e:

                    print(f"{symbol} ERROR : {e}")

                completed += 1

                if completed % 20 == 0 or completed == len(symbols):

                    print(

                        f"[{completed}/{len(symbols)}] "

                        f"Collected rows : {len(all_rows)}"

                    )

        # ----------------------------------------------------
        # Save
        # ----------------------------------------------------

        print("\nWriting to PostgreSQL...")

        inserted = save_all_rows(

            cur,

            conn,

            all_rows

        )

        print()

        print("=" * 60)
        print("UPDATE COMPLETED")
        print("=" * 60)

        print(f"Symbols checked : {len(symbols)}")
        print(f"Rows inserted   : {inserted}")

    finally:

        cur.close()

        conn.close()


# ============================================================
# ENTRY
# ============================================================

if __name__ == "__main__":

    main()