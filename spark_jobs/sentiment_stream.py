from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType
from textblob import TextBlob
import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

spark = SparkSession.builder \
    .appName("KalshiSentimentStream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
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
        return 0.0
    return float(TextBlob(text).sentiment.polarity)

sentiment_udf = udf(score_sentiment, FloatType())

def classify_sentiment(score):
    if score is None:
        return "neutral"
    if score > 0.05:
        return "positive"
    elif score < -0.05:
        return "negative"
    return "neutral"

classify_udf = udf(classify_sentiment, StringType())

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "reddit-posts") \
    .load()

parsed = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

scored = parsed \
    .withColumn("sentiment_score", sentiment_udf(col("text"))) \
    .withColumn("sentiment", classify_udf(col("sentiment_score")))

def write_to_snowflake(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
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
        cursor.execute("""
            INSERT INTO reddit_posts 
            (post_id, market_id, subreddit, text, upvotes, 
             timestamp, ingested_at, sentiment, sentiment_score)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row.post_id, row.market_id, row.subreddit,
            row.text, row.upvotes, row.timestamp,
            row.ingested_at, row.sentiment, row.sentiment_score
        ))
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Batch {batch_id}: wrote {len(rows)} rows to Snowflake")

scored.writeStream \
    .foreachBatch(write_to_snowflake) \
    .outputMode("append") \
    .start() \
    .awaitTermination()