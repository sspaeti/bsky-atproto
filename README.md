# bsky-atproto
This is my playground to learn about Bluesky AT Protocol.


Find also my curated list on:
- [Bsky Tools](https://www.ssp.sh/brain/bluesky/#tools): Existing tools to visualize, searching for starter packs etc.
- [AT Proto Tools](https://www.ssp.sh/brain/at-protocol#extracting-data-via-the-protocol): Tools to interact with the unterlaying protocol. Export data, attach to the firehose, etc.



## `streaming_into_duckdb.py`

To use this code:

Set up your environment variables:

```sh
export BSKY_USERNAME="your-handle.bsky.social"
export BSKY_PASSWORD="your-app-password"
```

Make sure you have all required packages:

```sh
pip install -r requirements.txt
```

Run the script:

```sh
python hashtag_monitor.py
```


The script will:
- Create a data directory for parquet files
- Fetch existing posts with the specified hashtags
- Monitor the firehose for new posts
- Write posts to parquet files in batches of 100
- Load the data into MotherDuck every minute


Query with:
```sql
-- Recent posts
SELECT * FROM posts 
ORDER BY created_at DESC 
LIMIT 10;

-- Posts by hashtag
SELECT hashtag, COUNT(*) as count 
FROM posts 
GROUP BY hashtag
ORDER BY count DESC;

-- Top authors
SELECT author, COUNT(*) as post_count 
FROM posts 
GROUP BY author 
ORDER BY post_count DESC
LIMIT 10;
```
