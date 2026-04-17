import requests

headers = {'User-Agent': 'kalshi-sentiment-bot/1.0'}

response = requests.get(
    'https://www.reddit.com/r/investing/search.json',
    headers=headers,
    params={
        'q': 'federal reserve',
        'sort': 'new',
        'limit': 5
    }
)

posts = response.json()['data']['children']
for post in posts:
    print(post['data']['title'])