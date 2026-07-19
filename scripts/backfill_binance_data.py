import os
import time
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

DB_URL = os.environ["DB_CONNECTION_STRING"]
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "BULLAUSDT"]  # thêm coin bạn cần
START_DATE = datetime(2023, 1, 1)
INTERVAL = "1h"

def get_klines_batch(symbol, start_time_ms):
    url = f"https://data-api.binance.vision/api/v3/klines?symbol={symbol}&interval={INTERVAL}&limit=1000&startTime={start_time_ms}"
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()

def backfill_symbol(symbol):
    start_ms = int(START_DATE.timestamp() * 1000)
    all_rows = []
    while True:
        batch = get_klines_batch(symbol, start_ms)
        if not batch:
            break
        all_rows.extend(batch)
        last_time = batch[-1][0]
        start_ms = last_time + 1
        print(f"{symbol}: fetched {len(batch)} rows, total {len(all_rows)}")
        if len(batch) < 1000:
            break  # đã lấy tới hiện tại
        time.sleep(0.3)  # tránh rate limit
    return all_rows

def save_batch(rows, symbol):
    if not rows:
        return
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    values = [
        (symbol, datetime.fromtimestamp(r[0]/1000), r[1], r[2], r[3], r[4], r[5])
        for r in rows
    ]
    execute_values(cur, """
        INSERT INTO fact_price_volume (symbol, open_time, open, high, low, close, volume)
        VALUES %s
        ON CONFLICT (symbol, open_time) DO UPDATE
        SET open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
            close=EXCLUDED.close, volume=EXCLUDED.volume
    """, values)
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    for sym in SYMBOLS:
        print(f"=== Backfilling {sym} ===")
        rows = backfill_symbol(sym)
        save_batch(rows, sym)
        print(f"Done {sym}: {len(rows)} total rows saved\n")
