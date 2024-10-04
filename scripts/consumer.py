import json
import random
import time
from typing import Dict, NamedTuple
import kafka
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from kafka import KafkaConsumer, KafkaProducer
import mlflow.spark


import os
os.environ["AWS_ACCESS_KEY_ID"] = "ACCESS_KEY"
os.environ["AWS_SECRET_ACCESS_KEY"] = "SECRET_ACCESS_KEY"
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "https://storage.yandexcloud.net"
os.environ["AWS_DEFAULT_REGION"] = "ru-central1"	

spark = SparkSession.builder \
    .appName("TransactionValidation") \
    .config("spark.hadoop.fs.s3a.access.key", "ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.secret.key", "SECRET_ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.yandexcloud.net") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

class RecordMetadata(NamedTuple):
    key: int
    topic: str
    partition: int
    offset: int

def set_or_create_experiment(experiment_name):
    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment is None:
        mlflow.create_experiment(experiment_name)
        print(f"Эксперимент '{experiment_name}' был создан.")
    mlflow.set_experiment(experiment_name)
    print(f"Эксперимент '{experiment_name}' установлен как активный.")
    return mlflow.set_experiment(experiment_name)

mlflow.set_tracking_uri("http://YOUR_MLFLOW_SERVER:8000")
experiment = set_or_create_experiment("lrmodel")

logged_model = 'runs:/YOUR_MODEL/lrModel'
loaded_model = mlflow.spark.load_model(logged_model)

# Kafka Consumer
consumer = KafkaConsumer(
    bootstrap_servers="YOUR_KAFKA",
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-512",
    sasl_plain_username="username",
    sasl_plain_password="password",
    ssl_cafile="CA.pem",
    value_deserializer=json.loads,
)
consumer.subscribe(topics=["test"])

# Kafka Producer for predictions
producer = KafkaProducer(
    bootstrap_servers="rc1d-hmuruc8ba4t0t6di.mdb.yandexcloud.net:9091",
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-512",
    sasl_plain_username="username",
    sasl_plain_password="password",
    ssl_cafile="CA.pem",
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

new_topic = "predictions"  # New topic for predictions

print("Waiting for new messages. Press Ctrl+C to stop")
try:
    for msg in consumer:
        print(f"{msg.topic}:{msg.partition}:{msg.offset}: key={msg.key} value={msg.value}")
        data = msg.value
        df = spark.createDataFrame([data])
        assembler = VectorAssembler(inputCols=['tx_datetime', 'tx_amount', 'tx_time_seconds', 'tx_time_days', 'avg_transaction_count_1', 'avg_transaction_mean_1', 'avg_transaction_count_7', 'avg_transaction_mean_7', 'avg_transaction_count_30', 'avg_transaction_mean_30', 'avg_transaction_terminal_id_count_1', 'avg_transaction_terminal_id_count_7', 'avg_transaction_terminal_id_count_30'], outputCol='features')
        assembled_df = assembler.transform(df)
        scaler = MinMaxScaler(inputCol="features", outputCol="scaledFeatures")
        scaler_model = scaler.fit(assembled_df)
        scaled_df = scaler_model.transform(assembled_df)
        scaled_df.show()
        prediction = loaded_model.transform(scaled_df)
        
        # Send predictions to the new topic
        for row in prediction.select("prediction").collect():
            prediction_value = row.prediction
            producer.send(new_topic, {"prediction": prediction_value})
            print(f"Sent prediction: {prediction_value} to topic: {new_topic}")

except KeyboardInterrupt:
    pass
finally:
    producer.close()
