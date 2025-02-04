# Real-time MLOps pipeline using Spark Streaming and Kafka.
An MLOps pipeline made with the Diabetes Health Indicators Dataset from kaggle:
https://www.kaggle.com/datasets/alexteboul/diabetes-health-indicators-dataset

The dataset is split in to offline and online phase (80/20 split).  
A model is trained on the offline dataset and evaluated with cross validation getting the best model and saving it.

The online dataset is used to simulate real life interactions with the model. Data is sent without the target column to a kafka topic and using Spark streaming jobs a prediction is made and sent to the predictions topic.

A high level diagram is available in the MLOps-pipeline architecture file
