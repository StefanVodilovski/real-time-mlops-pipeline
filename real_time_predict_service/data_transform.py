from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import (
    LogisticRegression,
    RandomForestClassifier,
    GBTClassifier,
    OneVsRest,
)
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder


def clean_dataframe(dataframe):
    dataframe = dataframe.dropna()
    dataframe = dataframe.drop(
        "Fruits", "AnyHealthcare", "Veggies", "HvyAlcoholConsump"
    )
    return dataframe


def transform_dataframe(dataframe):
    feature_assembler = VectorAssembler(
        inputCols=[
            "HighBP",
            "HighChol",
            "CholCheck",
            "BMI",
            "Smoker",
            "Stroke",
            "HeartDiseaseorAttack",
            "PhysActivity",
            "NoDocbcCost",
            "GenHlth",
            "MentHlth",
            "PhysHlth",
            "DiffWalk",
            "Sex",
            "Age",
            "Education",
            "Income",
        ],
        outputCol="features",
    )
    if "Diabetes_012" in dataframe.columns:
        dataframe = feature_assembler.transform(dataframe).select(
            "features", "Diabetes_012"
        )
        dataframe = dataframe.withColumnRenamed("Diabetes_012", "label")
    else:
        dataframe = feature_assembler.transform(dataframe).select("features")
    return dataframe
