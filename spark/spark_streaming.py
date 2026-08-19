"""
PySpark Structured Streaming Sentiment Analysis
Reads real-time messages from Kafka topic 'sentiment_inputs',
applies Sentiment NLP / ML transformation, and outputs results
to Kafka topic 'sentiment_results' and console sink.
"""

import os
import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, struct, current_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    ArrayType
)

# Add parent and spark path to sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sentiment_engine import analyze_sentiment

# Define Input and Output Schemas
INPUT_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("text", StringType(), True),
    StructField("source", StringType(), True),
    StructField("user", StringType(), True),
    StructField("timestamp", StringType(), True)
])

OUTPUT_SCHEMA = StructType([
    StructField("sentiment", StringType(), True),
    StructField("emoji", StringType(), True),
    StructField("label", StringType(), True),
    StructField("polarity", DoubleType(), True),
    StructField("confidence", DoubleType(), True),
    StructField("positive_score", DoubleType(), True),
    StructField("negative_score", DoubleType(), True),
    StructField("keywords", ArrayType(StringType()), True)
])


def sentiment_udf_func(text):
    if text is None:
        text = ""
    res = analyze_sentiment(text)
    return (
        res["sentiment"],
        res["emoji"],
        res["label"],
        float(res["polarity"]),
        float(res["confidence"]),
        float(res["positive_score"]),
        float(res["negative_score"]),
        res["keywords"]
    )


# Register Spark UDF
spark_sentiment_udf = udf(sentiment_udf_func, OUTPUT_SCHEMA)


def start_spark_streaming():
    kafka_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    input_topic = "sentiment_inputs"
    output_topic = "sentiment_results"

    print("=" * 65)
    print("✨ STARTING PYSPARK STRUCTURED STREAMING SENTIMENT PIPELINE")
    print(f"📡 Kafka Servers: {kafka_servers}")
    print(f"📥 Inbound Topic: {input_topic}")
    print(f"📤 Outbound Topic: {output_topic}")
    print("=" * 65)

    spark = (
        SparkSession.builder
        .appName("RealTimeSentimentStreaming")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Read stream from Kafka
    df_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", input_topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON value
    df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), INPUT_SCHEMA).alias("data")) \
        .select("data.*")

    # Apply Sentiment ML Analysis
    df_analyzed = df_parsed.withColumn("analysis", spark_sentiment_udf(col("text"))) \
        .select(
            col("id"),
            col("text"),
            col("source"),
            col("user"),
            col("timestamp").alias("input_timestamp"),
            current_timestamp().alias("processed_at"),
            col("analysis.sentiment").alias("sentiment"),
            col("analysis.emoji").alias("emoji"),
            col("analysis.label").alias("label"),
            col("analysis.polarity").alias("polarity"),
            col("analysis.confidence").alias("confidence"),
            col("analysis.positive_score").alias("pos_score"),
            col("analysis.negative_score").alias("neg_score"),
            col("analysis.keywords").alias("keywords")
        )

    # Convert to JSON for Kafka outbound stream
    df_kafka_out = df_analyzed.select(
        col("id").alias("key"),
        struct(
            col("id"),
            col("text"),
            col("source"),
            col("user"),
            col("input_timestamp"),
            col("processed_at"),
            col("sentiment"),
            col("emoji"),
            col("label"),
            col("polarity"),
            col("confidence"),
            col("pos_score"),
            col("neg_score"),
            col("keywords")
        ).cast("string").alias("value")
    )

    # Console stream query for real-time terminal output
    console_query = (
        df_analyzed.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .start()
    )

    # Outbound Kafka query (if kafka topic exists)
    try:
        kafka_query = (
            df_kafka_out.writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_servers)
            .option("topic", output_topic)
            .option("checkpointLocation", "logs/spark_kafka_checkpoint")
            .outputMode("append")
            .start()
        )
        print("✅ Spark Streaming Kafka Sink & Console Sink initialized successfully!")
    except Exception as e:
        print(f"⚠️ Kafka write stream failed to start: {e}. Falling back to Console sink only.")

    console_query.awaitTermination()


if __name__ == "__main__":
    start_spark_streaming()
