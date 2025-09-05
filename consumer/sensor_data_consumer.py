import os
import time
import json
from threading import Thread
from kafka import KafkaConsumer
from kafka.errors import KafkaError

THREAD_COUNT = int(os.getenv("THREAD_COUNT", 4))

class SensorDataConsumer:
    def __init__(self, topic="plc_data", thread_id=0):
        self.topic = topic
        self.thread_id = thread_id
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
        print(f"THREAD {self.thread_id}: received: {msg}")

def generate_consumer(thread_id):
    consumer = SensorDataConsumer(thread_id=thread_id)
    consumer.start_consuming()

if __name__ == "__main__":
    threads = []

    for i in range(THREAD_COUNT):
        t = Thread(target=generate_consumer, args=(i,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()
