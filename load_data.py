from pyspark.sql import SparkSession
import os

# Tell Spark where Hadoop configuration is
os.environ["HADOOP_CONF_DIR"] = "/home/saumya/hadoop-3.3.6/etc/hadoop"

spark = (
    SparkSession.builder
    .appName("Load Sentiment Dataset")
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
    .getOrCreate()
)

df = (
    spark.read
    .option("inferSchema", True)
    .csv("hdfs://localhost:9000/sentiment/training.1600000.processed.noemoticon.csv")
)

print("Rows:", df.count())

df.show(5, truncate=False)

spark.stop()


