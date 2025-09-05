import time
import random
import json

class SensorDataProducer:
    def __init__(self, machines, sensors, interval_ms=1000):
        self.machines = machines
        self.sensors = sensors
        self.interval_ms = interval_ms

    def generate_reading(self, interval=(0,100)):
        return {
            "machine_id": random.choice(self.machines),
            "sensor": random.choice(self.sensors),
            "reading": random.uniform(interval[0], interval[1]),
            "timestamp": time.time()
        }

    def start(self, num_readings=None):
        count = 0
        while True:
            data = self.generate_reading()
            print(data)
            count += 1
            if num_readings and count >= num_readings:
                break
            time.sleep(self.interval_ms / 1000)

if __name__ == "__main__":
    producer = SensorDataProducer(
        machines=['M1', 'M2', 'M3'],
        sensors=['temperature', 'pressure', 'vibration'],
        interval_ms=500
    )
    producer.start(num_readings=20)
