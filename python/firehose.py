from atproto import (
    FirehoseSubscribeReposClient,
    parse_subscribe_repos_message,
    CAR,
    models
)
from typing import Optional
import datetime

class FirehoseSubscriber:
    def __init__(self):
        self.client = FirehoseSubscribeReposClient()
        
    def start(self):
        """Start the firehose subscription"""
        print("Starting firehose subscription...")
        self.client.start(self.on_message_handler)

    def on_message_handler(self, message) -> None:
        """Handle incoming firehose messages"""
        # Parse the message
        commit = parse_subscribe_repos_message(message)
        
        # We only want to process commit messages that have blocks
        if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
            return
            
        if not commit.blocks:
            return
            
        try:
            # Decode the CAR file containing the actual data
            car = CAR.from_bytes(commit.blocks)
            
            # Get the operations (creates, deletes, etc.)
            ops = commit.ops
            
            # Process each operation
            for op in ops:
                # Get the record data from the CAR file
                record = None
                if op.action == 'create' or op.action == 'update':
                    record = car.blocks.get(op.cid)
                
                self._process_operation(op, record)
                
        except Exception as e:
            print(f"Error processing message: {e}")
    
    def _process_operation(self, op, record: Optional[dict]) -> None:
        """Process individual operations from the firehose"""
        timestamp = datetime.datetime.now().isoformat()
        
        # Print basic operation info
        print(f"\n[{timestamp}] New operation:")
        print(f"Action: {op.action}")
        print(f"Path: {op.path}")
        
        # If we have a record, print its details
        if record:
            print("Record data:")
            if op.path.startswith('app.bsky.feed.post'):
                # Handle posts
                if 'text' in record:
                    print(f"Post text: {record['text']}")
                if 'createdAt' in record:
                    print(f"Created at: {record['createdAt']}")
            elif op.path.startswith('app.bsky.feed.like'):
                # Handle likes
                if 'subject' in record:
                    print(f"Liked post: {record['subject']}")
            elif op.path.startswith('app.bsky.feed.repost'):
                # Handle reposts
                if 'subject' in record:
                    print(f"Reposted: {record['subject']}")
        print("-" * 50)

def main():
    subscriber = FirehoseSubscriber()
    try:
        subscriber.start()
    except KeyboardInterrupt:
        print("\nShutting down firehose subscription...")

if __name__ == "__main__":
    main()
