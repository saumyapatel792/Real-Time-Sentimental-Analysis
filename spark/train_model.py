from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, rand

from pyspark.ml.feature import (
    Tokenizer,
    StopWordsRemover,
    HashingTF,
    IDF
)

from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# ----------------------------------------------------
# Create Spark Session
# ----------------------------------------------------

spark = (
    SparkSession.builder
    .appName("Sentiment Analysis Training")
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("=" * 60)
print("Loading Dataset")
print("=" * 60)

# ----------------------------------------------------
# Read Dataset
# ----------------------------------------------------

df = spark.read.csv(
    "hdfs://localhost:9000/sentiment/training.1600000.processed.noemoticon.csv",
    inferSchema=True
)

df = df.toDF(
    "target",
    "id",
    "date",
    "flag",
    "user",
    "text"
)

print("Original Dataset Size :", df.count())

# ----------------------------------------------------
# Convert Labels
# 0 -> Negative
# 4 -> Positive
# ----------------------------------------------------

df = df.withColumn(
    "label",
    when(col("target") == 4, 1).otherwise(0)
)

df = df.select("text", "label")

# ----------------------------------------------------
# Randomly shuffle dataset
# ----------------------------------------------------

df = df.orderBy(rand())

# ----------------------------------------------------
# OPTIONAL:
# For quick testing use 100000 rows
# Remove this line for full dataset training
# ----------------------------------------------------

df = df.limit(100000)

print("Training Dataset Size :", df.count())

# ----------------------------------------------------
# Split Dataset
# ----------------------------------------------------

train, test = df.randomSplit([0.8, 0.2], seed=42)

print("Train :", train.count())
print("Test  :", test.count())

# ----------------------------------------------------
# NLP Pipeline
# ----------------------------------------------------

tokenizer = Tokenizer(
    inputCol="text",
    outputCol="words"
)

stopwords = StopWordsRemover(
    inputCol="words",
    outputCol="filtered"
)

tf = HashingTF(
    inputCol="filtered",
    outputCol="rawFeatures",
    numFeatures=20000
)

idf = IDF(
    inputCol="rawFeatures",
    outputCol="features"
)

lr = LogisticRegression(
    labelCol="label",
    featuresCol="features",
    maxIter=10
)

pipeline = Pipeline(stages=[
    tokenizer,
    stopwords,
    tf,
    idf,
    lr
])

print("\nTraining Model...\n")

model = pipeline.fit(train)

print("Model Training Completed")

# ----------------------------------------------------
# Save Model
# ----------------------------------------------------

print("\nSaving Model...")

model.write().overwrite().save("models/sentiment_model")

print("Model Saved Successfully")

# ----------------------------------------------------
# Prediction
# ----------------------------------------------------

prediction = model.transform(test)

prediction.select(
    "text",
    "label",
    "prediction"
).show(20, truncate=False)

# ----------------------------------------------------
# Accuracy
# ----------------------------------------------------

evaluator = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="accuracy"
)

accuracy = evaluator.evaluate(prediction)

print("=" * 60)
print("Accuracy :", round(accuracy * 100, 2), "%")
print("=" * 60)

spark.stop()


