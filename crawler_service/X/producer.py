from kafka import KafkaProducer
import json 
from decouple import config


KAFKA_BOOTSTRAP_SERVERS = config("KAFKA_BOOTSTRAP_SERVERS")
class KafkaTweetProducer:
    def __init__(self, topic: str, bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8")  # serialize key
        )
        self.topic = topic

    def send(self, data: dict):
        key = data.get("keyword", "default")
        self.producer.send(self.topic, key=key, value=data)
        self.producer.flush()
