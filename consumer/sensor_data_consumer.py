import os
import time
import json
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime 
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

POSTGRES_USER = os.getenv("POSTGRES_USER", "user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "example")
POSTGRES_DB = os.getenv("POSTGRES_DB", "plc_data")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "db")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 100))
BATCH_TIMEOUT = float(os.getenv("BATCH_TIMEOUT", 2))

SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
KAFKA_URL = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "plc_data")

class SensorDataConsumer:
    def __init__(self, topic="plc_data"):
        self.topic = topic
        self.batch = []
        self.last_flush = time.time()
        self.total_msgs_received = 0
        self.total_msgs_flushed = 0

        while True:
            try:
                with open("schema/schema.avsc") as f:
                    value_schema = f.read()
                schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

                self.avro_deserializer = AvroDeserializer(
                    schema_registry_client,
                    value_schema
                )
                print("successfully connected to schema registry")
                break
            except Exception as e:
                print(f"retrying connection to schema registry in 2 seconds... {e}")
                time.sleep(2)
        while True:
            try:
                consumer_config = {
                    "bootstrap.servers": KAFKA_URL,
                    "group.id": f"{KAFKA_TOPIC}_group",
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": False
                }

                self.consumer = Consumer(consumer_config)
                self.consumer.subscribe([self.topic])
                print("successfully connected to broker")
                break
            except Exception as e:
                print(f"retrying kafka connection in 2 seconds... {e}")
                time.sleep(2)
        while True:
            try:
                self.conn = psycopg2.connect(
                    host=POSTGRES_HOST,
                    port=POSTGRES_PORT,
                    user=POSTGRES_USER,
                    password=POSTGRES_PASSWORD,
                    dbname=POSTGRES_DB
                )
                self.cursor = self.conn.cursor()
                print("successfully connected to postgres")
                break
            except psycopg2.OperationalError:
                print("retrying postgres connection in 3 seconds...")
                time.sleep(3)

    def start_consuming(self):
        print(f"listening for messages on topic {self.topic}...")
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    if self.batch and (len(self.batch) >= BATCH_SIZE or (time.time() - self.last_flush) >= BATCH_TIMEOUT):
                        self.send_batch()
                        print(f"TOTAL RECEIVED: {self.total_msgs_received}, TOTAL FLUSHED: {self.total_msgs_flushed}")
                        self.consumer.commit()
                    continue
                if msg.error():
                    print(f"consumer error: {msg.error()}")
                    continue

                try:
                    deserialized_value = self.avro_deserializer(
                        msg.value(),
                        SerializationContext(msg.topic(), MessageField.VALUE)
                    )
                    self.handle_message(deserialized_value)
                    self.total_msgs_received += 1
                except Exception as e:
                    print(f"CONSUMER: failed to deserialize: {e}")

                #TODO: duplicated code from line 85, turn into a function
                if self.batch and (len(self.batch) >= BATCH_SIZE or (time.time() - self.last_flush) >= BATCH_TIMEOUT):
                    self.send_batch()
                    print(f"TOTAL RECEIVED: {self.total_msgs_received}, TOTAL FLUSHED: {self.total_msgs_flushed}")
                    self.consumer.commit()

        except Exception as e:
            print(f"error in consumption loop: {e}")
        finally:
            self.send_batch()
            self.consumer.commit()

    def handle_message(self, msg):
        self.batch.append((
            msg["machine_id"],
            msg["sensor"],
            msg["reading"],
            msg["t_stamp"],
        ))

    def send_batch(self):
        if not self.batch:
            return
        insert_query = """
        INSERT INTO raw_data.sensor_data(machine_id, sensor, reading, t_stamp)
        VALUES %s
        """
        formatted_batch = [
            (machine_id, sensor, reading, datetime.fromtimestamp(t_stamp))
            for machine_id, sensor, reading, t_stamp in self.batch
        ]
        try:
            # self.cursor.executemany(insert_query, self.batch)
            execute_values(self.cursor, insert_query, formatted_batch)
            self.conn.commit()
            print(f"flushed {len(self.batch)} records to postgres at {time.time()}")
            self.total_msgs_flushed += len(self.batch)
        except Exception as e:
            print(f"ERROR inserting batch of {len(self.batch)}: {e}")
            self.conn.rollback()
        self.batch.clear()
        self.last_flush = time.time()

if __name__ == "__main__":
    consumer = SensorDataConsumer(topic=KAFKA_TOPIC)
    consumer.start_consuming()