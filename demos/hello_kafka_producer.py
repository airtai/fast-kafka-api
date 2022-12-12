from fast_kafka_api.application import FastKafkaAPI
from fast_kafka_api.asyncapi import KafkaMessage
from pydantic import Field
from os import environ
import time
import asyncio
import threading

kafka_server_url = environ["KAFKA_HOSTNAME"]
kafka_server_port = environ["KAFKA_PORT"]

kafka_config = {
        "bootstrap.servers": f"{kafka_server_url}:{kafka_server_port}",
        "group.id": "hello_producer_group"
    }

class HelloKafkaMsg(KafkaMessage):
    msg: str = Field(
        ...,
        example="Hello",
        description="Demo hello world message",
    )

app = FastKafkaAPI(
        kafka_config=kafka_config
    )

@app.produces
def on_hello(msg: HelloKafkaMsg, kafka_msg: Any):
    print(f"Sending hello msg: {msg}")

async def greet_kafka():
    while(True):
        msg = HelloKafkaMsg(msg = "hello")
        app.produce("hello", msg)
        await asyncio.sleep(2)

# @app.on_event("startup")
# async def schedule_periodic():
#     loop = asyncio.get_event_loop()
#     loop.create_task(greet_kafka())

asyncio.create_task(greet_kafka())