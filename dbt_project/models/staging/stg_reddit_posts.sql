with source as (
    select * from kalshi_sentiment.raw.reddit_posts
),

cleaned as (
    select
        post_id,
        market_id,
        subreddit,
        text,
        upvotes,
        sentiment,
        sentiment_score,
        to_timestamp_ntz(TIMESTAMP::integer) as post_timestamp,
        to_timestamp_ntz(INGESTED_AT::integer) as ingested_at,
        datediff(
            'second',
            to_timestamp_ntz(TIMESTAMP::integer),
            to_timestamp_ntz(INGESTED_AT::integer)
        ) as pipeline_latency_seconds
    from source
    where post_id is not null
      and text is not null
      and sentiment in ('positive', 'negative', 'neutral')
    qualify row_number() over (partition by post_id order by ingested_at asc) = 1
)

select * from cleaned