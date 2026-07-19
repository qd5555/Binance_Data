import os
import time
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

DB_URL = os.environ["DB_CONNECTION_STRING"]
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
START_DATE = datetime(2025, 1, 1)

def get_funding_batch(symbol, start_time_ms):
    url = f"https://fapi.binance.com/fapi/v1/fundingRate?symbol={symbol}&limit=1000&startTime={start_time_ms}"
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()

def backfill_symbol(symbol):
    start_ms = int(START_DATE.timestamp() * 1000)
    all_rows = []
    while True:
        batch = get_funding_batch(symbol, start_ms)
        if not batch:
            break
        all_rows.extend(batch)
        last_time = batch[-1]["fundingTime"]
        start_ms = last_time + 1
        print(f"{symbol}: fetched {len(batch)} rows, total {len(all_rows)}")
        if len(batch) < 1000:
            break
        time.sleep(0.3)
    return all_rows

def save_batch(rows, symbol):
    if not rows:
        print(f"{symbol}: no data (possibly not available on Futures)")
        return
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    values = [
        (symbol, float(r["fundingRate"]), datetime.fromtimestamp(r["fundingTime"]/1000))
        for r in rows
    ]
    execute_values(cur, """
        INSERT INTO fact_funding_rate (symbol, funding_rate, funding_time)
        VALUES %s
        ON CONFLICT (symbol, funding_time) DO UPDATE
        SET funding_rate = EXCLUDED.funding_rate
    """, values)
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    for sym in SYMBOLS:
        print(f"=== Backfilling funding rate: {sym} ===")
        try:
            rows = backfill_symbol(sym)
            save_batch(rows, sym)
            print(f"Done {sym}: {len(rows)} total rows saved\n")
        except requests.exceptions.HTTPError as e:
            print(f"ERROR {sym}: {e}\n")