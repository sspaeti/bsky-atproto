from atproto import (
    FirehoseSubscribeReposClient,
    parse_subscribe_repos_message,
    CAR,
    models,
    Client
)
from typing import Optional, Set, List
import datetime
import logging
import asyncio
import re
import os

class HashtagMonitor:
    def __init__(self, *hashtags: str):
        """
        Initialize the hashtag monitor
        
        Args:
            *hashtags: Variable number of hashtags to monitor (without the # symbol)
        """
        self.client = FirehoseSubscribeReposClient()
        self.bsky_client = Client()
        self.hashtags = [tag.lower() for tag in hashtags]
        self.known_posts: Set[str] = set()
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
    async def initialize(self):
        """Initialize by fetching existing hashtag posts"""
        try:
            # Load environment variables and login
            username = os.getenv('BSKY_USERNAME')
            password = os.getenv('BSKY_PASSWORD')
            
            if not username or not password:
                raise ValueError("BSKY_USERNAME and BSKY_PASSWORD environment variables must be set")
                
            profile = self.bsky_client.login(username, password)
            self.logger.info(f"Logged in as @{profile.handle}")
            
            hashtag_posts: List[dict] = []
            
            # Fetch posts for each hashtag
            for hashtag in self.hashtags:
                self.logger.info(f"\nFetching existing #{hashtag} posts...")
                cursor = None
                
                while True:
                    fetched = self.bsky_client.app.bsky.feed.search_posts(
                        params={'q': f'#{hashtag}', 'cursor': cursor, 'limit': 25}
                    )
                    
                    for post in fetched.posts:
                        if post.uri not in self.known_posts:  # Avoid duplicates across hashtags
                            self.known_posts.add(post.uri)
                            hashtag_posts.append({
                                'uri': post.uri,
                                'text': post.record.text,
                                'author': post.author.handle,
                                'timestamp': post.indexed_at,
                                'cid': post.cid,
                                'hashtag': hashtag
                            })
                    
                    if not fetched.cursor:
                        break
                        
                    cursor = fetched.cursor
                
            self.logger.info(f"\nInitialized with {len(hashtag_posts)} unique posts containing: " +
                           ", ".join(f"#{tag}" for tag in self.hashtags))
            
            # Log the most recent posts
            self.logger.info("\nMost recent posts:")
            for post in sorted(hashtag_posts, key=lambda x: x['timestamp'], reverse=True)[:5]:
                self.logger.info("\n" + "="*50)
                self.logger.info(f"[{post['timestamp']}] New post (#{post['hashtag']}):")
                self.logger.info(f"Author: @{post['author']}")
                self.logger.info(f"Content: {post['text']}")
                self.logger.info(f"URI: {post['uri']}")
                self.logger.info(f"CID: {post['cid']}")
                self.logger.info("="*50)
            
        except Exception as e:
            self.logger.error(f"Error initializing hashtag monitor: {e}")
            raise

    def start(self):
        """Start the firehose subscription"""
        hashtags_str = ", ".join(f"#{tag}" for tag in self.hashtags)
        self.logger.info(f"\nStarting live monitoring for: {hashtags_str}...")
        self.client.start(self.on_message_handler)

    def on_message_handler(self, message) -> None:
        """Handle incoming firehose messages"""
        commit = parse_subscribe_repos_message(message)
        
        if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
            return
            
        if not commit.blocks:
            return
            
        try:
            # Decode the CAR file
            car = CAR.from_bytes(commit.blocks)
            
            # Process each operation
            for op in commit.ops:
                record = None
                if op.action == 'create':  # We only care about new posts
                    record = car.blocks.get(op.cid)
                
                if record:
                    self._process_post(commit.repo, op, record)
                
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
    
    def _process_post(self, repo: str, op, record: dict) -> None:
        """Process posts and check for hashtags"""
        try:
            if not op.path.startswith('app.bsky.feed.post'):
                return
                
            # Check if we've seen this post before
            post_uri = f"at://{repo}/{op.path}"
            if post_uri in self.known_posts:
                return
                
            # Check for hashtags in text
            if 'text' in record:
                text = record['text'].lower()
                matching_hashtags = [tag for tag in self.hashtags if f'#{tag}' in text]
                
                if matching_hashtags:
                    self._handle_hashtag_post(repo, op, record, post_uri, matching_hashtags)
                    
        except Exception as e:
            self.logger.error(f"Error in post processing: {e}")

    def _handle_hashtag_post(self, repo: str, op, record: dict, post_uri: str, matching_hashtags: List[str]) -> None:
        """Handle posts containing our hashtags"""
        timestamp = datetime.datetime.now().isoformat()
        hashtags_str = ", ".join(f"#{tag}" for tag in matching_hashtags)
        
        self.logger.info("\n" + "="*50)
        self.logger.info(f"[{timestamp}] New post detected with {hashtags_str}:")
        self.logger.info(f"Action: {op.action}")
        self.logger.info(f"Author: @{repo}")
        
        # Post content
        if 'text' in record:
            self.logger.info(f"Content: {record['text']}")
        
        # Created timestamp
        if 'createdAt' in record:
            self.logger.info(f"Created at: {record['createdAt']}")
        
        # Additional metadata
        self.logger.info(f"URI: {post_uri}")
        self.logger.info(f"CID: {op.cid}")
        
        # Embedded content
        if 'embed' in record:
            self.logger.info("\nEmbedded content:")
            self.logger.info(record['embed'])
        
        # Languages
        if 'langs' in record:
            self.logger.info(f"\nLanguage(s): {', '.join(record['langs'])}")
        
        # Facets (mentions, links, etc.)
        if 'facets' in record:
            self.logger.info("\nFacets:")
            for facet in record['facets']:
                self.logger.info(facet)
        
        self.logger.info("="*50)
        
        # Add to known posts
        self.known_posts.add(post_uri)

async def main():
    # Initialize the monitor with multiple hashtags
    monitor = HashtagMonitor('databs', 'datasky')
    
    try:
        # Initialize with existing posts
        await monitor.initialize()
        
        # Start monitoring firehose
        monitor.start()
        
        # Keep the script running
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        logging.info("\nShutting down hashtag monitor...")

if __name__ == "__main__":
    asyncio.run(main())
