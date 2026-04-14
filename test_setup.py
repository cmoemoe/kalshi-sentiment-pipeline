from dotenv import load_dotenv
import praw
import os

load_dotenv()

reddit = praw.Reddit(
    client_id=os.getenv('REDDIT_CLIENT_ID'),
    client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
    user_agent=os.getenv('REDDIT_USER_AGENT')
)

for post in reddit.subreddit('investing').new(limit=3):
    print(post.title)