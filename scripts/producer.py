import json
import random
import time
from typing import Dict, NamedTuple

import kafka
from faker import Faker

fake = Faker()

from pyspark.sql.functions import rand
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("Read Parquet Files") \
    .getOrCreate()


class RecordMetadata(NamedTuple):
    key: int
    topic: str
    partition: int
    offset: int


def main():
    kafka_server = "rc1d-hmuruc8ba4t0t6di.mdb.yandexcloud.net:9091"

    producer = kafka.KafkaProducer(
        bootstrap_servers=kafka_server,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username="username",
        sasl_plain_password="password",
        ssl_cafile="CA.pem",
        value_serializer=serialize,
    )

    try:
        while True:
            record_md = send_message(producer, "test")
            print(
                f"Msg sent. Key: {record_md.key}, topic: {record_md.topic}, partition:{record_md.partition}, offset:{record_md.offset}"
            )
    except KeyboardInterrupt:
        print(" KeyboardInterrupted!")
        producer.flush()
        producer.close()


def send_message(producer: kafka.KafkaProducer, topic: str) -> RecordMetadata:
    data = spark.read.parquet("reeditdata")
    df_with_random = data.withColumn("random", rand())
    random_row = df_with_random.orderBy("random").limit(1)
    random_row.show()
    random_row_data = random_row.collect()[0]
    click = generate_click(random_row_data.asDict())
    future = producer.send(
        topic=topic,
        key=str(click["tx_datetime"]).encode("ascii"),
        value=click,
    )

    record_metadata = future.get(timeout=1)
    return RecordMetadata(
        key=click["tx_datetime"],
        topic=record_metadata.topic,
        partition=record_metadata.partition,
        offset=record_metadata.offset,
    )


def generate_click(row):
    return {
        "tx_datetime": row['tx_datetime'],
        "tx_amount": row['tx_amount'],
        "tx_time_seconds": row['tx_time_seconds'],
        "tx_time_days": row['tx_time_days'],
        "avg_transaction_count_1": row['avg_transaction_count_1'],
        "avg_transaction_mean_1": row['avg_transaction_mean_1'],
        "avg_transaction_count_7": row['avg_transaction_count_7'],
        "avg_transaction_mean_7": row['avg_transaction_mean_7'],
        "avg_transaction_count_30": row['avg_transaction_count_30'],
        "avg_transaction_mean_30": row['avg_transaction_mean_30'],
        "avg_transaction_terminal_id_count_1": row['avg_transaction_terminal_id_count_1'],
        "avg_transaction_terminal_id_count_7": row['avg_transaction_terminal_id_count_7'],
        "avg_transaction_terminal_id_count_30": row['avg_transaction_terminal_id_count_30'],
    }



def serialize(msg: Dict) -> bytes:
    return json.dumps(msg).encode("utf-8")


if __name__ == "__main__":
    main()
