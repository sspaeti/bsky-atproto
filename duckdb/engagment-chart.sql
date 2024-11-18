-- Charting bars with engagement metrics
WITH raw_data AS (
  SELECT * FROM read_json_auto('https://public.api.bsky.app/xrpc/app.bsky.feed.getAuthorFeed?actor=did:plc:edglm4muiyzty2snc55ysuqx&limit=100')
),
unnested_feed AS (
  SELECT unnest(feed) as post_data FROM raw_data
),
engagement_data AS (
  SELECT 
    RIGHT(post_data.post.uri, 13) as post_uri,
    --post_data.post.uri as post_uri,
    post_data.post.author.handle,
    LEFT(post_data.post.record.text, 50) as post_text,
    post_data.post.record.createdAt as created_at,
    (post_data.post.replyCount + 
     post_data.post.repostCount + 
     post_data.post.likeCount + 
     post_data.post.quoteCount) as total_engagement,
      post_data.post.replyCount as replies,
      post_data.post.repostCount as reposts,
      post_data.post.likeCount as likes,
      post_data.post.quoteCount as quotes,
  FROM unnested_feed
)
SELECT 
  post_uri,
  created_at,
  total_engagement,
  bar(total_engagement, 0, 
      (SELECT MAX(total_engagement) FROM engagement_data), 
      30) as engagement_chart,
  replies, reposts, likes, quotes,
  post_text,

FROM engagement_data
WHERE handle = 'ssp.sh'
ORDER BY total_engagement DESC
LIMIT 30;
