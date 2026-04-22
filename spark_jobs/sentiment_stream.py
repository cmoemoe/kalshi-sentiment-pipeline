from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType
from textblob import TextBlob
import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

spark = SparkSession.builder \
    .appName("KalshiSentimentStream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
    .config("spark.local.dir", "C:/spark-tmp") \
    .config("spark.driver.extraJavaOptions", "-Djava.io.tmpdir=C:/spark-tmp") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("post_id", StringType()),
    StructField("market_id", StringType()),
    StructField("subreddit", StringType()),
    StructField("text", StringType()),
    StructField("upvotes", LongType()),
    StructField("timestamp", FloatType()),
    StructField("ingested_at", FloatType())
])

def score_sentiment(text):
    if not text:
        return 0.0, "neutral"
    score = float(TextBlob(text).sentiment.polarity)
    if score > 0.05:
        label = "positive"
    elif score < -0.05:
        label = "negative"
    else:
        label = "neutral"
    return score, label

def write_to_snowflake(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        print(f"Batch {batch_id}: no rows to write")
        return

    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
    )
    cursor = conn.cursor()

    for row in rows:
        sentiment_score, sentiment_label = score_sentiment(row.text)
        cursor.execute("""
            INSERT INTO kalshi_sentiment.raw.reddit_posts
            (post_id, market_id, subreddit, text, upvotes,
             timestamp, ingested_at, sentiment, sentiment_score)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row.post_id, row.market_id, row.subreddit,
            row.text, row.upvotes, row.timestamp,
            row.ingested_at, sentiment_label, sentiment_score
        ))

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Batch {batch_id}: wrote {len(rows)} rows to Snowflake")

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "reddit-posts") \
    .load()

parsed = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

parsed.writeStream \
    .foreachBatch(write_to_snowflake) \
    .outputMode("append") \
    .start() \
    .awaitTermination()