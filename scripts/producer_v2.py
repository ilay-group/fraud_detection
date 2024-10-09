import json
from typing import Dict, NamedTuple
from pydantic import BaseModel
import kafka
from kafka import KafkaConsumer, KafkaProducer
from fastapi import FastAPI, HTTPException

# Initialize FastAPI
app = FastAPI()

class RecordMetadata(NamedTuple):
    key: int
    topic: str
    partition: int
    offset: int

import os

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


consumer.subscribe(topics=["predictions"])



# Define the data model for the request body
class ClickData(BaseModel):
    tx_datetime: int
    tx_amount: float
    tx_time_seconds: int
    tx_time_days: int
    avg_transaction_count_1: int
    avg_transaction_mean_1: float
    avg_transaction_count_7: int
    avg_transaction_mean_7: float
    avg_transaction_count_30: int
    avg_transaction_mean_30: float
    avg_transaction_terminal_id_count_1: int
    avg_transaction_terminal_id_count_7: int
    avg_transaction_terminal_id_count_30: int

@app.post("/send-message/")
async def send_message(click_data: ClickData):
    try:
        click = click_data.dict()
        future = producer.send(
            topic="test",
            key=str(click["tx_datetime"]).encode("ascii"),
            value=click,
        )
        
        record_metadata = future.get(timeout=1)
        
        for msg in consumer:
           print(f"{msg.topic}:{msg.partition}:{msg.offset}: key={msg.key} value={msg.value}")
           return {
            f"{msg.topic}:{msg.partition}:{msg.offset}: key={msg.key} value={msg.value}"
           }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
