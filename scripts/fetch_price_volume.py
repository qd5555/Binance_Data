import os
import time
import requests
import psycopg2

from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from psycopg2.extras import execute_values

# ============================================================
# CONFIG
# ============================================================

DB_URL = os.environ["DB_CONNECTION_STRING"]

INTERVAL = "1d"

BINANCE_URL = "https://data-api.binance.vision/api/v3/klines"

MAX_WORKERS = 20

REQUEST_TIMEOUT = 20

MAX_RETRY = 3

SLEEP_BETWEEN_RETRY = 2

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


def get_last_candle(cur):

    cur.execute("""
        SELECT
            symbol,
            MAX(open_time)
        FROM fact_price_volume
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
# BINANCE
# ============================================================

def request_with_retry(url):

    for attempt in range(MAX_RETRY):

        try:

            r = requests.get(
                url,
                timeout=REQUEST_TIMEOUT
            )

            r.raise_for_status()

            return r.json()

        except Exception:

            if attempt == MAX_RETRY - 1:
                raise

            time.sleep(SLEEP_BETWEEN_RETRY)

    return []


def fetch_symbol(symbol, last_timestamp):

    url = (
        f"{BINANCE_URL}"
        f"?symbol={symbol}"
        f"&interval={INTERVAL}"
        f"&limit=2"
    )

    if last_timestamp:

        url += f"&startTime={last_timestamp}"

    try:

        rows = request_with_retry(url)

        values = []

        for r in rows:

            values.append(
                (
                    symbol,
                    datetime.utcfromtimestamp(r[0] / 1000),
                    r[1],
                    r[2],
                    r[3],
                    r[4],
                    r[5]
                )
            )

        return values

    except Exception as e:

        print(f"{symbol} : FAILED ({e})")

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
    print("BINANCE DAILY UPDATE")
    print("=" * 60)

    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    try:

        # ---------------------------------------------
        # Load symbols
        # ---------------------------------------------

        symbols = get_symbols(cur)

        print(f"Trading symbols : {len(symbols)}")

        # ---------------------------------------------
        # Load last candle
        # ---------------------------------------------

        last_timestamp = get_last_candle(cur)

        print("Last candle map loaded.")

        # ---------------------------------------------
        # Download concurrently
        # ---------------------------------------------

        all_rows = []

        completed = 0

        print("\nDownloading new candles...\n")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

            futures = {

                executor.submit(
                    fetch_symbol,
                    symbol,
                    last_timestamp.get(symbol)
                ): symbol

                for symbol in symbols

            }

            for future in as_completed(futures):

                symbol = futures[future]

                rows = future.result()

                if rows:

                    all_rows.extend(rows)

                completed += 1

                if completed % 20 == 0 or completed == len(symbols):

                    print(
                        f"[{completed}/{len(symbols)}] "
                        f"Collected rows : {len(all_rows)}"
                    )

        # ---------------------------------------------
        # Save
        # ---------------------------------------------

        print("\nWriting to PostgreSQL...")

        inserted = save_all_rows(
            cur,
            conn,
            all_rows
        )

        print("\n")
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