import os
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

DB_URL = os.environ["DB_CONNECTION_STRING"]
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]

def get_last_timestamp(symbol, cur):
    cur.execute("SELECT MAX(funding_time) FROM fact_funding_rate WHERE symbol = %s", (symbol,))
    result = cur.fetchone()[0]
    if result:
        return int(result.timestamp() * 1000) + 1
    return None

def get_new_funding(symbol, start_time_ms):
    url = f"https://fapi.binance.com/fapi/v1/fundingRate?symbol={symbol}&limit=1000"
    if start_time_ms:
        url += f"&startTime={start_time_ms}"
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()

def save_batch(rows, symbol, cur):
    if not rows:
        return
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

if __name__ == "__main__":
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    for sym in SYMBOLS:
        last_ts = get_last_timestamp(sym, cur)
        data = get_new_funding(sym, last_ts)
        save_batch(data, sym, cur)
        conn.commit()
        print(f"{sym}: +{len(data)} new rows")
    cur.close()
    conn.close()