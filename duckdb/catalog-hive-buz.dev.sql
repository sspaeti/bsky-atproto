attach 'https://hive.buz.dev/bluesky/catalog' as bsky;
select count(*) from bsky.jetstream;

select * from bsky.jetstream LIMIT 100;

