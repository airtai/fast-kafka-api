from fast_kafka_api.application import FastKafkaAPI
from fast_kafka_api.asyncapi import KafkaMessage
from pydantic import Field
from os import environ

kafka_server_url = environ["KAFKA_HOSTNAME"]
kafka_server_port = environ["KAFKA_PORT"]

kafka_config = {
        "bootstrap.servers": f"{kafka_server_url}:{kafka_server_port}",
        "group.id": "hello_copnsumer_group"
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

@app.consumes
async def on_hello(msg: HelloKafkaMsg):
    print(f"Got data, msg={msg.msg}")