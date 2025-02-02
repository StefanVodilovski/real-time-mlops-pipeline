from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
import os
from pyspark.sql.types import StructType, StructField, FloatType
from data_transform import *
from pyspark.ml.classification import LogisticRegressionModel

spark = (
    SparkSession.builder.appName("RealTimePrediction")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.4")
    .master("local[4]")
    .getOrCreate()
)


kafka_broker = "localhost:9092"
kafka_topic_input = "health_data"
kafka_topic_output = "health_data_predicted"
checkpoint_path = "./checkpoint_kafka_predictions"

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_broker)
    .option("subscribe", kafka_topic_input)
    .load()
)

# Define the schema for the JSON data
json_schema = StructType(
    [
        StructField("HighBP", FloatType(), True),
        StructField("HighChol", FloatType(), True),
        StructField("CholCheck", FloatType(), True),
        StructField("BMI", FloatType(), True),
        StructField("Smoker", FloatType(), True),
        StructField("Stroke", FloatType(), True),
        StructField("HeartDiseaseorAttack", FloatType(), True),
        StructField("PhysActivity", FloatType(), True),
        StructField("Fruits", FloatType(), True),
        StructField("Veggies", FloatType(), True),
        StructField("HvyAlcoholConsump", FloatType(), True),
        StructField("AnyHealthcare", FloatType(), True),
        StructField("NoDocbcCost", FloatType(), True),
        StructField("GenHlth", FloatType(), True),
        StructField("MentHlth", FloatType(), True),
        StructField("PhysHlth", FloatType(), True),
        StructField("DiffWalk", FloatType(), True),
        StructField("Sex", FloatType(), True),
        StructField("Age", FloatType(), True),
        StructField("Education", FloatType(), True),
        StructField("Income", FloatType(), True),
    ]
)

df = (
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) AS value")
    .withColumn("json_data", from_json(col("value"), json_schema))
    .select("key", "json_data.*")
)

path = "../models/best_model"
model = LogisticRegressionModel.load(path)

df = clean_dataframe(df)
df = transform_dataframe(df)


predictions = model.transform(df)


predictions = predictions.withColumnRenamed("prediction", "label")
predictions = predictions.selectExpr("to_json(struct(*)) AS value")


query = (
    predictions.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_broker)
    .option("topic", kafka_topic_output)
    .option(
        "checkpointLocation", checkpoint_path
    )  # Checkpointing is required for Kafka output
    .outputMode("append")
    .start()
)

query.awaitTermination()
