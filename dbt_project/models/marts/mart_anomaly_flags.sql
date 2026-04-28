with hourly as (
    select * from {{ ref('int_sentiment_windows') }}
),

stats as (
    select
        market_id,
        avg(avg_sentiment_score) as mean_sentiment,
        stddev(avg_sentiment_score) as stddev_sentiment
    from hourly
    group by market_id
),

flagged as (
    select
        h.market_id,
        h.window_hour,
        h.avg_sentiment_score,
        h.post_count,
        s.mean_sentiment,
        s.stddev_sentiment,
        abs(h.avg_sentiment_score - s.mean_sentiment) / nullif(s.stddev_sentiment, 0) as z_score
    from hourly h
    join stats s on h.market_id = s.market_id
)

select * from flagged
where z_score > 2
order by window_hour desc
