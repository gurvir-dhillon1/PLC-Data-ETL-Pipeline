import os
import time
import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError

class SensorDataConsumer:
    def __init__(self, topic="plc_data"):
        self.topic = topic
        while True:
            try:
                self.consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092"),
                    auto_offset_reset="earliest",
                    group_id="plc-data-group",
                    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
                )
                print("successfully connected to broker")
                break
            except KafkaError:
                print("retrying in 3 seconds...")
                time.sleep(3)

    def start_consuming(self):
        print(f"listening for messages on topic {self.topic}...")
        for msg in self.consumer:
            self.handle_message(msg.value)


    def handle_message(self, msg):
        print(f"received: {msg}")

if __name__ == "__main__":
    consumer = SensorDataConsumer()
    consumer.start_consuming()
