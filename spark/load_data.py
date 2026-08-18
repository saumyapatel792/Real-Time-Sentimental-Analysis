from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Load Sentiment Dataset") \
    .getOrCreate()

df = spark.read.csv(
    "hdfs:///sentiment/training.1600000.processed.noemoticon.csv",
    inferSchema=True
)

print("Total Records :", df.count())

df.show(10, truncate=False)

spark.stop()


