import requests
import json
import time
from datetime import datetime

SOURCE = "Hacker News"
NEW_STORIES_URL = "https://hacker-news.firebaseio.com/v0/newstories.json"
ITEM_URL = "https://hacker-news.firebaseio.com/v0/item/{}.json"

def fetch_posts(limit=25):
    story_ids = requests.get(NEW_STORIES_URL).json()[:limit]
    posts = []
    for sid in story_ids:
        item = requests.get(ITEM_URL.format(sid)).json()
        if item and "title" in item:
            posts.append(item)
    return posts

def main():
    print(f"PulseLite started — watching {SOURCE}")
    print("-" * 50)

    while True:
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Fetching posts...")
        posts = fetch_posts()
        print(f"Fetched {len(posts)} posts")

        for post in posts:
            print(f"  Title: {post['title'][:60]}")
            print(f"  Score: {post.get('score', 0)} | Comments: {post.get('descendants', 0)}")
            print()

        print("Waiting 30 seconds...")
        time.sleep(30)

if __name__ == "__main__":
    main()