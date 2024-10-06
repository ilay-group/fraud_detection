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

aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
mlflow_s3_endpoint_url = os.getenv("MLFLOW_S3_ENDPOINT_URL")
aws_default_region = os.getenv("AWS_DEFAULT_REGION")


os.environ["AWS_ACCESS_KEY_ID"] = aws_access_key_id
os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret_access_key
os.environ["MLFLOW_S3_ENDPOINT_URL"] = mlflow_s3_endpoint_url
os.environ["AWS_DEFAULT_REGION"] = aws_default_region

spark = SparkSession.builder \
    .appName("TransactionValidation") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
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
MLFLOW_TRACKING_URI=os.getenv("MLFLOW_TRACKING_URI")
mlflow.set_tracking_uri(f"http://{MLFLOW_TRACKING_URI}")
experiment = set_or_create_experiment("lrmodel")

logged_model = 'runs:/b958bca33fc847feb63e40ee4ab074c3/lrModel'
loaded_model = mlflow.spark.load_model(logged_model)

kafka_server = str(os.getenv("BOOTSTRAP_SERVERS", ""))
kafka_username = str(os.getenv("KAFKA_USERNAME", ""))
kafka_password = str(os.getenv("KAFKA_PASSWORD", ""))

producer = KafkaProducer(
    bootstrap_servers=kafka_server,
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-512",
    sasl_plain_username=kafka_username,
    sasl_plain_password=kafka_password,
    ssl_cafile="CA.pem",
    value_serializer=lambda msg: json.dumps(msg).encode("utf-8"),
)

consumer = KafkaConsumer(
    bootstrap_servers=kafka_server,
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-512",
    sasl_plain_username=kafka_username,
    sasl_plain_password=kafka_password,
    ssl_cafile="CA.pem",
    value_deserializer=json.loads,
)


consumer.subscribe(topics=["test"])

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