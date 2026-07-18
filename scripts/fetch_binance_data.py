import os
import requests
import psycopg2
from datetime import datetime

DB_URL = os.environ["DB_CONNECTION_STRING"]

def get_klines(symbol="BTCUSDT", interval="1h", limit=1000):
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()

def save_to_db(data, symbol):
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    for row in data:
        open_time = datetime.fromtimestamp(row[0] / 1000)
        cur.execute("""
            INSERT INTO fact_price_volume (symbol, open_time, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, open_time) DO UPDATE
            SET open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume
        """, (symbol, open_time, row[1], row[2], row[3], row[4], row[5]))
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    symbols = ["BTCUSDT", "ETHUSDT"]  # thêm coin bạn cần
    for sym in symbols:
        print(f"Fetching {sym}...")
        data = get_klines(sym)
        save_to_db(data, sym)
        print(f"Saved {len(data)} rows for {sym}")