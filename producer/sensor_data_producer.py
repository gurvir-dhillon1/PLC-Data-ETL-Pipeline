import os
import time
import random
import json
from threading import Thread
from kafka import KafkaProducer
from kafka.errors import KafkaError

class SensorDataProducer:
    def __init__(self, machines, sensors, interval_ms=1000):
        self.machines = machines
        self.sensors = sensors
        self.interval_ms = interval_ms
        while True:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092"),   # initial broker(s) to discover the Kafka cluster
                    value_serializer=lambda v: json.dumps(v).encode("utf-8")    # automatically converts data to JSON bytes
                )
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

    def send(self, num_readings=None):
        count = 0
        while True:
            data = self.generate_reading()
            self.producer.send("plc_data", value=data)
            count += 1
            if num_readings and count >= num_readings:
                break
            time.sleep(self.interval_ms / 1000)
        self.producer.flush()

def generate_producer(machines, sensors, interval_ms):
    producer = SensorDataProducer(
        machines=machines,
        sensors=sensors,
        interval_ms=interval_ms
    )
    producer.send()

if __name__ == "__main__":
    machines=['M1', 'M2', 'M3']
    sensors=['temperature', 'pressure', 'vibration']
    interval_ms=500

    threads = []
    for i in range(4):
        t = Thread(target=generate_producer, args=(machines,sensors,interval_ms))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()
