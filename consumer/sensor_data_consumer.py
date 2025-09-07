import os
import time
import json
import psycopg2
from kafka import KafkaConsumer
from kafka.errors import KafkaError

POSTGRES_USER = os.getenv("POSTGRES_USER", "user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "example")
POSTGRES_DB = os.getenv("POSTGRES_DB", "plc_data")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "db")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 100))
BATCH_TIMEOUT = float(os.getenv("BATCH_TIMEOUT", 2))

class SensorDataConsumer:
    def __init__(self, topic="plc_data"):
        self.topic = topic
        self.batch = []
        self.last_flush = time.time()
        self.total_msgs_received = 0
        self.total_msgs_flushed = 0
        

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
                msg_pack = self.consumer.poll(timeout_ms=1000)
                if msg_pack:
                    for tp, messages in msg_pack.items():
                        for msg in messages:
                            self.handle_message(msg.value)
                    if self.batch and (len(self.batch) >= BATCH_SIZE or (time.time() - self.last_flush) >= BATCH_TIMEOUT):
                        self.send_batch()
                        print(f"TOTAL: {self.total_msgs_flushed}")
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
            msg["timestamp"],
            msg["message_id"],
            msg["sequence"]
        ))

    def send_batch(self):
        if not self.batch:
            return
        insert_query = """
        INSERT INTO raw_data.sensor_data(machine_id, sensor, reading, t_stamp, message_id, sequence)
        VALUES (%s, %s, %s, to_timestamp(%s), %s, %s)
        """
        try:
            self.cursor.executemany(insert_query, self.batch)
            self.conn.commit()
            print(f"flushed {len(self.batch)} records to postgres at {time.time()}")
            self.total_msgs_flushed += len(self.batch)
        except Exception as e:
            print(f"ERROR inserting batch of {len(self.batch)}: {e}")
            self.conn.rollback()
        self.batch.clear()
        self.last_flush = time.time()

if __name__ == "__main__":
    consumer = SensorDataConsumer()
    consumer.start_consuming()