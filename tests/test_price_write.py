import requests
import snowflake.connector
import time
import os
from dotenv import load_dotenv

load_dotenv()

BASE_URL = 'https://api.elections.kalshi.com/trade-api/v2'

TRACKED_MARKETS = [
    'KXRATECUTCOUNT-26DEC31-T0',
    'KXRATECUTCOUNT-26DEC31-T1',
    'KXRATECUTCOUNT-26DEC31-T2',
    'KXRATECUTCOUNT-26DEC31-T3'
]

def fetch_market(ticker):
    response = requests.get(f'{BASE_URL}/markets/{ticker}')
    if response.status_code == 200:
        market = response.json().get('market', {})
        return {
            'market_id': ticker,
            'market_title': market.get('title', ''),
            'yes_bid': float(market.get('yes_bid_dollars', 0) or 0),
            'yes_ask': float(market.get('yes_ask_dollars', 0) or 0),
            'volume': float(market.get('volume_fp', 0) or 0),
            'timestamp': time.time()
        }
    return None

conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
)
cursor = conn.cursor()

for ticker in TRACKED_MARKETS:
    price = fetch_market(ticker)
    if price:
        cursor.execute("""
            INSERT INTO kalshi_sentiment.raw.kalshi_prices
            (market_id, market_title, yes_bid, yes_ask, volume, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            price['market_id'], price['market_title'],
            price['yes_bid'], price['yes_ask'],
            price['volume'], price['timestamp']
        ))
        print(f"Wrote: {price['market_title'][:50]} | Yes bid: {price['yes_bid']}")

conn.commit()
cursor.close()
conn.close()
print("Done.")