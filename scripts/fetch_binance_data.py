import os
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

DB_URL = os.environ["DB_CONNECTION_STRING"]
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
INTERVAL = "1h"

def get_last_timestamp(symbol, cur):
    cur.execute("SELECT MAX(open_time) FROM fact_price_volume WHERE symbol = %s", (symbol,))
    result = cur.fetchone()[0]
    if result:
        return int(result.timestamp() * 1000) + 1
    return None

def get_new_klines(symbol, start_time_ms):
    url = f"https://data-api.binance.vision/api/v3/klines?symbol={symbol}&interval={INTERVAL}&limit=1000"
    if start_time_ms:
        url += f"&startTime={start_time_ms}"
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()

def save_batch(rows, symbol, cur):
    if not rows:
        return
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

if __name__ == "__main__":
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    for sym in SYMBOLS:
        last_ts = get_last_timestamp(sym, cur)
        data = get_new_klines(sym, last_ts)
        save_batch(data, sym, cur)
        conn.commit()
        print(f"{sym}: +{len(data)} new rows")
    cur.close()
    conn.close()
