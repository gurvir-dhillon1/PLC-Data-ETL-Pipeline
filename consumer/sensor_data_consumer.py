import os
import time
import json
import psycopg2
from threading import Thread
from kafka import KafkaConsumer
from kafka.errors import KafkaError

THREAD_COUNT = int(os.getenv("THREAD_COUNT", 4))
POSTGRES_USER = os.getenv("POSTGRES_USER", "user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "example")
POSTGRES_DB = os.getenv("POSTGRES_DB", "plc_data")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "db")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 100))
BATCH_TIMEOUT = float(os.getenv("BATCH_TIMEOUT", 2))

class SensorDataConsumer:
    def __init__(self, topic="plc_data", thread_id=0):
        self.topic = topic
        self.thread_id = thread_id
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
                    consumer_timeout_ms=6000,
                    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
                )
                print(f"THREAD {self.thread_id}: successfully connected to broker")
                break
            except KafkaError:
                print(f"THREAD {self.thread_id}: retrying in 3 seconds...")
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
                print(f"THREAD {self.thread_id}: successfully connected to postgres")
                break
            except psycopg2.OperationalError:
                print(f"THREAD {self.thread_id}: retrying postgres connection in 3 seconds...")
                time.sleep(3)

    def start_consuming(self):
        print(f"listening for messages on topic {self.topic}...")
        try:
            for msg in self.consumer:
                self.handle_message(msg.value)

                if len(self.batch) >= BATCH_SIZE or time.time() - self.last_flush >= BATCH_TIMEOUT:
                    self.send_batch()
                    print(f"TOTAL: {self.total_msgs_flushed}")
                    self.consumer.commit()
        except Exception as e:
            print(f"THREAD {self.thread_id}: error in consumption loop: {e}")

    def handle_message(self, msg):
        self.batch.append((
            msg["machine_id"],
            msg["sensor"],
            msg["reading"],
            msg["timestamp"],
            msg["message_id"],
            msg["thread_id"],
            msg["sequence"]
        ))

    def send_batch(self):
        if not self.batch:
            return
        insert_query = """
        INSERT INTO raw_data.sensor_data(machine_id, sensor, reading, t_stamp, message_id, thread_id, sequence)
        VALUES (%s, %s, %s, to_timestamp(%s), %s, %s, %s)
        """
        try:
            self.cursor.executemany(insert_query, self.batch)
            self.conn.commit()
            print(f"THREAD {self.thread_id}: flushed {len(self.batch)} records to postgres at {time.time()}")
            self.total_msgs_flushed += len(self.batch)
        except Exception as e:
            print(f"THREAD {self.thread_id}: ERROR inserting batch of {len(self.batch)}: {e}")
            self.conn.rollback()
        self.batch.clear()
        self.last_flush = time.time()

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
