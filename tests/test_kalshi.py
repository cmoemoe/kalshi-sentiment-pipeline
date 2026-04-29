import requests
import json

BASE_URL = 'https://api.elections.kalshi.com/trade-api/v2'

response = requests.get(
    f'{BASE_URL}/events/KXRATECUTCOUNT-26DEC31',
    params={'with_nested_markets': 'true'}
)
print("Status:", response.status_code)
print(json.dumps(response.json(), indent=2))
