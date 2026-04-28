with base as (
    select * from {{ ref('stg_reddit_posts') }}
),

windowed as (
    select
        market_id,
        subreddit,
        date_trunc('hour', post_timestamp) as window_hour,
        count(*) as post_count,
        avg(sentiment_score) as avg_sentiment_score,
        sum(case when sentiment = 'positive' then 1 else 0 end) as positive_count,
        sum(case when sentiment = 'negative' then 1 else 0 end) as negative_count,
        sum(case when sentiment = 'neutral' then 1 else 0 end) as neutral_count,
        sum(upvotes) as total_upvotes,
        avg(upvotes * sentiment_score) as weighted_sentiment
    from base
    group by market_id, subreddit, date_trunc('hour', post_timestamp)
)

select * from windowed