import os
import time
import random
from threading import Thread
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


THREAD_COUNT = int(os.getenv("THREAD_COUNT", 4))
INDIVIDUAL_THREAD_MSGS = int(os.getenv("INDIVIDUAL_THREAD_MSGS", 20))

INTERVAL_MS = int(os.getenv("INTERVAL_MS", 500))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 16384))   # 16 KB
LINGER_MS = int(os.getenv("LINGER_MS", 10))

SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
KAFKA_URL = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "plc_data")
SCHEMA_REGISTRY_SUBJECT = f"{KAFKA_TOPIC}_value"

class SensorDataProducer:
    def __init__(self, machines, sensors, interval_ms=1000):
        self.machines = machines
        self.sensors = sensors
        self.interval_ms = interval_ms
        while True:
            try:
                with open("schema/schema.avsc") as f:
                    value_schema = f.read()
                schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
                avro_serializer = AvroSerializer(
                    schema_registry_client,
                    value_schema
                )
                producer_config = {
                    "bootstrap.servers": KAFKA_URL,
                    "batch.size": BATCH_SIZE,
                    "linger.ms": LINGER_MS,
                    "enable.idempotence": True,
                    "acks": "all",
                    "retries": 5
                }
                self.producer = Producer(producer_config)
                self.avro_serializer = avro_serializer
                print("PRODUCER: successfully connected to kafka cluster and schema registry")
                break
            except Exception as e:
                print(f"PRODUCER: trying to connect to broker again in 2 seconds... {e}")
                time.sleep(2)

    def generate_reading(self, interval=(0,100)):
        return {
            "machine_id": random.choice(self.machines),
            "sensor": random.choice(self.sensors),
            "reading": random.uniform(interval[0], interval[1]),
            "t_stamp": time.time()
        }

    def send(self, thread_id, num_readings=None):
        for i in range(num_readings):
            data = self.generate_reading()
            try:
                serialized_value = self.avro_serializer(data, SerializationContext(KAFKA_TOPIC, MessageField.VALUE))
                self.producer.produce(
                    topic=KAFKA_TOPIC,
                    value=serialized_value,
                    callback=self.delivery_callback
                )
                self.producer.poll(0)
            except Exception as e:
                print(f"THREAD {i}: send failed: {e}")
            time.sleep(INTERVAL_MS / 1000)
        print(f"THREAD {thread_id}: finished sending {num_readings} messages")
    
    def delivery_callback(self, err, msg):
        """callback for message delivery reports"""
        if err is not None:
            print(f"message delivery failed: {err}")
        else:
            print(f"message delivered to {msg.topic()} [{msg.partition()}]")

    def run(self):
        threads = []
        for i in range(THREAD_COUNT):
            t = Thread(target=self.send, args=(i,INDIVIDUAL_THREAD_MSGS))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()

        self.producer.flush()
        self.producer.close(timeout=10)
        print(f"PRODUCER: ALL THREADS FINISHED")


if __name__ == "__main__":
    machines=['M1', 'M2', 'M3']
    sensors=['temperature', 'pressure', 'vibration']

    producer = SensorDataProducer(
        machines=machines,
        sensors=sensors
    )
    producer.run()
