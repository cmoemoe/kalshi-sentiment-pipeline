import requests
import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

MARKET_SUBREDDIT_MAP = {
    'KXRATECUTCOUNT-26DEC31': {
        'subreddits': ['investing', 'economics', 'wallstreetbets', 'stocks'],
        'keywords': [
            'rate cuts 2026',
            'federal reserve cuts',
            'fomc cuts',
            'how many rate cuts',
            'number of rate cuts',
            'fed cuts this year',
            'interest rate outlook',
            'rate cut expectations'
        ]
    }
}

headers = {'User-Agent': 'kalshi-sentiment-bot/1.0'}
seen_ids = set()

def is_relevant(text, keywords):
    text_lower = text.lower()
    return any(f' {kw} ' in f' {text_lower} ' for kw in keywords)

def fetch_posts(subreddit, keyword):
    try:
        response = requests.get(
            f'https://www.reddit.com/r/{subreddit}/search.json',
            headers=headers,
            params={
                'q': keyword,
                'sort': 'new',
                'limit': 50
            },
            timeout=10
        )
        if response.status_code == 200:
            return response.json()['data']['children']
        return []
    except Exception as e:
        print(f"Request error for {subreddit}/{keyword}: {e}")
        return []

def poll_and_publish():
    for market_id, config in MARKET_SUBREDDIT_MAP.items():
        for subreddit in config['subreddits']:
            for keyword in config['keywords']:
                posts = fetch_posts(subreddit, keyword)
                for post in posts:
                    data = post['data']
                    post_id = data.get('id')
                    if post_id in seen_ids:
                        continue
                    text = data.get('title', '') + ' ' + data.get('selftext', '')
                    if not is_relevant(text, config['keywords']):
                        continue
                    seen_ids.add(post_id)
                    message = {
                        'post_id': post_id,
                        'market_id': market_id,
                        'subreddit': subreddit,
                        'text': data.get('title', '') + ' ' + data.get('selftext', ''),
                        'upvotes': data.get('score', 0),
                        'timestamp': data.get('created_utc'),
                        'ingested_at': time.time()
                    }
                    producer.send('reddit-posts', value=message)
                    print(f"Published: {data.get('title', '')[:60]}")
                time.sleep(2)

while True:
    print("Polling Reddit...")
    poll_and_publish()
    print("Sleeping 30 seconds...")
    time.sleep(30)