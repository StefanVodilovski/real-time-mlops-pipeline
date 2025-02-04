import json
import time
import pandas as pd
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    api_version=(3, 6, 0),  # Match Kafka broker version
)

csv_file_path = "./data/online.csv"
online_df = pd.read_csv(csv_file_path).drop("Diabetes_012", axis=1)

for index, row in online_df.iterrows():
    # Convert to Spark-compatible format
    message = {
        "HighBP": float(row["HighBP"]),
        "HighChol": float(row["HighChol"]),
        "CholCheck": float(row["CholCheck"]),
        "BMI": float(row["BMI"]),
        "Smoker": float(row["Smoker"]),
        "Stroke": float(row["Stroke"]),
        "HeartDiseaseorAttack": float(row["HeartDiseaseorAttack"]),
        "PhysActivity": float(row["PhysActivity"]),
        "Fruits": float(row["Fruits"]),
        "Veggies": float(row["Veggies"]),
        "HvyAlcoholConsump": float(row["HvyAlcoholConsump"]),
        "AnyHealthcare": float(row["AnyHealthcare"]),
        "NoDocbcCost": float(row["NoDocbcCost"]),
        "GenHlth": float(row["GenHlth"]),
        "MentHlth": float(row["MentHlth"]),
        "PhysHlth": float(row["PhysHlth"]),
        "DiffWalk": float(row["DiffWalk"]),
        "Sex": float(row["Sex"]),
        "Age": float(row["Age"]),
        "Education": float(row["Education"]),
        "Income": float(row["Income"]),
    }

    producer.send("health_data", value=message)
    print(f"Sent: {message}")
    time.sleep(0.5)  # Fixed delay instead of random

producer.flush()
