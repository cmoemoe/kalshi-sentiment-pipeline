with sentiment as (
    select * from {{ ref('int_sentiment_windows') }}
),

prices as (
    select
        market_id,
        market_title,
        to_timestamp_ntz(timestamp::integer) as price_timestamp,
        date_trunc('hour', to_timestamp_ntz(timestamp::integer)) as price_hour,
        yes_bid,
        volume
    from kalshi_sentiment.raw.kalshi_prices
),

-- compute implied expected cuts from price weighted average
expected_cuts as (
    select
        price_hour,
        sum(yes_bid * cast(replace(regexp_substr(market_id, 'T[0-9]+'), 'T', '') as integer)) as implied_expected_cuts,
        max(case when market_id = 'KXRATECUTCOUNT-26DEC31-T1' then yes_bid end) as prob_1_cut,
        max(case when market_id = 'KXRATECUTCOUNT-26DEC31-T2' then yes_bid end) as prob_2_cuts,
        max(case when market_id = 'KXRATECUTCOUNT-26DEC31-T3' then yes_bid end) as prob_3_cuts
    from prices
    group by price_hour
),

joined as (
    select
        s.window_hour,
        s.avg_sentiment_score,
        s.post_count,
        s.weighted_sentiment,
        e.implied_expected_cuts,
        e.prob_1_cut,
        e.prob_2_cuts,
        e.prob_3_cuts
    from sentiment s
    left join expected_cuts e
        on s.window_hour = e.price_hour
)

select * from joined
order by window_hour desc