with hourly as (
    select * from {{ ref('int_sentiment_windows') }}
),

daily as (
    select
        market_id,
        date_trunc('day', window_hour) as summary_date,
        sum(post_count) as total_posts,
        avg(avg_sentiment_score) as daily_avg_sentiment,
        sum(positive_count) as total_positive,
        sum(negative_count) as total_negative,
        sum(neutral_count) as total_neutral,
        sum(total_upvotes) as total_upvotes,
        stddev(avg_sentiment_score) as sentiment_volatility
    from hourly
    group by market_id, date_trunc('day', window_hour)
)

select * from daily