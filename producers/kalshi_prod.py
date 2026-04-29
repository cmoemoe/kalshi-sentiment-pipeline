import requests
import json
import time
import os
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

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

while True:
    for ticker in TRACKED_MARKETS:
        price = fetch_market(ticker)
        if price:
            producer.send('kalshi-prices', value=price)
            print(f"Published: {price['market_title']} | Yes bid: {price['yes_bid']}")
        time.sleep(2)
    print("Sleeping 60 seconds...")
    time.sleep(60)