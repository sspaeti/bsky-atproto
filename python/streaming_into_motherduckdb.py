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
import pandas as pd
import duckdb
import threading
import time

def test_motherduck_connection():
    """Test MotherDuck connection and loading"""
    try:
        con = duckdb.connect('md:bsky')
        print("Connected to MotherDuck successfully")
        
        # Try to create a simple table
        con.sql("CREATE OR REPLACE TABLE test (id INTEGER)")
        con.sql("INSERT INTO test VALUES (1)")
        result = con.sql("SELECT * FROM test").fetchone()
        print("Successfully created and queried test table")
        
        # Clean up
        con.sql("DROP TABLE test")
        con.close()
        return True
    except Exception as e:
        print(f"Error testing MotherDuck connection: {e}")
        return False

class HashtagMonitor:
    def __init__(self, *hashtags: str, output_dir: str = "data"):
        """
        Initialize the hashtag monitor
        
        Args:
            *hashtags: Variable number of hashtags to monitor (without the # symbol)
            output_dir: Directory to store parquet files
        """
        self.client = FirehoseSubscribeReposClient()
        self.bsky_client = Client()
        self.hashtags = [tag.lower() for tag in hashtags]
        self.known_posts: Set[str] = set()
        self.posts_buffer = []
        self.output_dir = output_dir
        self.batch_size = 700  # Write to parquet every 100 posts
        self.running = True
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(message)s')
        self.logger = logging.getLogger(__name__)
        
    def _write_batch_to_parquet(self):
        """Write buffered posts to parquet file"""
        if not self.posts_buffer:
            return
            
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.output_dir}/posts_{timestamp}.parquet"
        
        df = pd.DataFrame(self.posts_buffer)
        df.to_parquet(filename)
        
        self.logger.info(f"Wrote {len(self.posts_buffer)} posts to {filename}")
        self.posts_buffer = []

    def _add_post_to_buffer(self, post_data: dict):
        """Add a post to the buffer and write to parquet if batch size is reached"""
        self.posts_buffer.append(post_data)
        
        if len(self.posts_buffer) >= self.batch_size:
            self._write_batch_to_parquet()

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
            
            # Fetch posts for each hashtag
            for hashtag in self.hashtags:
                self.logger.info(f"\nFetching existing #{hashtag} posts...")
                cursor = None
                
                while True:
                    fetched = self.bsky_client.app.bsky.feed.search_posts(
                        params={'q': f'#{hashtag}', 'cursor': cursor, 'limit': 25}
                    )
                    
                    for post in fetched.posts:
                        if post.uri not in self.known_posts:
                            self.known_posts.add(post.uri)
                            
                            # Safely handle langs attribute
                            langs = None
                            if hasattr(post.record, 'langs') and post.record.langs:
                                try:
                                    langs = ','.join(post.record.langs)
                                except:
                                    langs = None
                            
                            post_data = {
                                'uri': post.uri,
                                'cid': post.cid,
                                'author': post.author.handle,
                                'text': post.record.text,
                                'created_at': post.record.created_at,
                                'indexed_at': post.indexed_at,
                                'hashtag': hashtag,
                                'langs': langs
                            }
                            
                            self._add_post_to_buffer(post_data)
                    
                    if not fetched.cursor:
                        break
                        
                    cursor = fetched.cursor
            
            # Write any remaining posts
            self._write_batch_to_parquet()
            
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
        try:
            commit = parse_subscribe_repos_message(message)
            
            # Skip messages that aren't commits or don't have blocks
            if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
                return
                
            # Handle case where message is too big
            if hasattr(commit, 'tooBig') and commit.tooBig:
                self.logger.debug("Skipping too big message")
                return
                
            if not hasattr(commit, 'blocks') or not commit.blocks:
                return
                
            try:
                car = CAR.from_bytes(commit.blocks)
                
                for op in commit.ops:
                    if op.action == 'create':
                        record = car.blocks.get(op.cid)
                        if record:
                            self._process_post(commit.repo, op, record)
                    
            except Exception as e:
                self.logger.error(f"Error processing message blocks: {e}")
                
        except Exception as e:
            self.logger.debug(f"Error parsing message: {e}")
    
    def _process_post(self, repo: str, op, record: dict) -> None:
        """Process posts and check for hashtags"""
        try:
            if not op.path.startswith('app.bsky.feed.post'):
                return
                
            post_uri = f"at://{repo}/{op.path}"
            if post_uri in self.known_posts:
                return
                
            if 'text' in record:
                text = record['text'].lower()
                matching_hashtags = [tag for tag in self.hashtags if f'#{tag}' in text]
                
                if matching_hashtags:
                    self._handle_hashtag_post(repo, op, record, post_uri, matching_hashtags)
                    
                    for hashtag in matching_hashtags:
                        # Safely handle langs
                        langs = None
                        if 'langs' in record and record['langs']:
                            try:
                                langs = ','.join(record['langs'])
                            except:
                                langs = None
                        
                        post_data = {
                            'uri': post_uri,
                            'cid': op.cid,
                            'author': repo,
                            'text': record['text'],
                            'created_at': record.get('createdAt'),
                            'hashtag': hashtag,
                            'langs': langs
                        }
                        
                        self._add_post_to_buffer(post_data)
                    
                    self.known_posts.add(post_uri)
                    
        except Exception as e:
            self.logger.error(f"Error in post processing: {e}")

    def _handle_hashtag_post(self, repo: str, op, record: dict, post_uri: str, matching_hashtags: List[str]) -> None:
        """Handle posts containing our hashtags"""
        timestamp = datetime.datetime.now().isoformat()
        hashtags_str = ", ".join(f"#{tag}" for tag in matching_hashtags)
        
        self.logger.info("\n" + "="*50)
        self.logger.info(f"[{timestamp}] New post detected with {hashtags_str}:")
        self.logger.info(f"Author: @{repo}")
        self.logger.info(f"Content: {record['text']}")
        self.logger.info(f"URI: {post_uri}")
        self.logger.info("="*50)

    def load_to_motherduck(self):
        """Load parquet files into MotherDuck"""
        try:
            con = duckdb.connect('md:bsky')
            
            # Create posts table from parquet files
            con.sql("""
                CREATE OR REPLACE TABLE posts AS 
                SELECT * FROM read_parquet('data/*.parquet')
            """)
            
            result = con.sql("SELECT COUNT(*) as count FROM posts").fetchone()
            self.logger.info(f"Loaded {result[0]} posts into MotherDuck")
        except Exception as e:
            self.logger.error(f"Error loading to MotherDuck: {e}")
        finally:
            if 'con' in locals():
                con.close()

    def periodic_task(self):
        """Run periodic tasks in a separate thread"""
        while self.running:
            try:
                self._write_batch_to_parquet()
                self.load_to_motherduck()
            except Exception as e:
                self.logger.error(f"Error in periodic task: {e}")
            time.sleep(60)  # Wait for 60 seconds

async def main():
    # Test MotherDuck connection first
    # if not test_motherduck_connection():
    #     print("Failed to connect to MotherDuck. Please check your connection.")
    #     return

    # Initialize the monitor with multiple hashtags
    monitor = HashtagMonitor('databs', 'datasky')
    
    try:
        # Initialize with existing posts
        await monitor.initialize()
        
        # Start periodic task in a separate thread
        periodic_thread = threading.Thread(target=monitor.periodic_task)
        periodic_thread.daemon = True
        periodic_thread.start()
        
        # Start firehose monitoring
        monitor.start()
        
        # Keep the main loop running
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        logging.info("\nShutting down hashtag monitor...")
        monitor.running = False
        monitor._write_batch_to_parquet()
        monitor.load_to_motherduck()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
