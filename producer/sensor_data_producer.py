import os
import time
import random
import json
from threading import Thread
from kafka import KafkaProducer
from kafka.errors import KafkaError

THREAD_COUNT = int(os.getenv("THREAD_COUNT", 4))
INTERVAL_MS = int(os.getenv("INTERVAL_MS", 500))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 16384))   # 16 KB
LINGER_MS = int(os.getenv("LINGER_MS", 10))
INDIVIDUAL_THREAD_MSGS = int(os.getenv("INDIVIDUAL_THREAD_MSGS", 20))

class SensorDataProducer:
    def __init__(self, machines, sensors, interval_ms=1000):
        self.machines = machines
        self.sensors = sensors
        self.interval_ms = interval_ms
        while True:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092"),   # initial broker(s) to discover the Kafka cluster
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),    # automatically converts data to JSON bytes
                    batch_size=BATCH_SIZE,
                    linger_ms=LINGER_MS,
                    enable_idempotence=True,
                    acks="all",
                    retries=5
                )
                print("PRODUCER: successfully connected to kafka cluster")
                break
            except KafkaError:
                print("PRODUCER: trying to connect to broker again in 3 seconds...")
                time.sleep(3)

    def generate_reading(self, interval=(0,100)):
        return {
            "machine_id": random.choice(self.machines),
            "sensor": random.choice(self.sensors),
            "reading": random.uniform(interval[0], interval[1]),
            "timestamp": time.time()
        }

    def send(self, thread_id, num_readings=None):
        for i in range(num_readings):
            data = self.generate_reading()
            data["message_id"] = f"thread_{thread_id}_msg_{i+1}"
            data["thread_id"] = thread_id
            data["sequence"] = i + 1
            future = self.producer.send("plc_data", value=data)
            try:
                future.get(timeout=5)
            except Exception as e:
                print(f"THREAD {i}: send failed: {e}")
            time.sleep(INTERVAL_MS / 1000)
        print(f"THREAD {thread_id}: finished sending {num_readings} messages")

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
