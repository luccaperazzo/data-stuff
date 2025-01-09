from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_sensor_data():
    return {
        "sensor_id": random.randint(1, 100),
        "temperature": round(random.uniform(20.0, 50.0), 2),
        "humidity": round(random.uniform(30.0, 90.0), 2),
        "timestamp": time.time()
    }

try:
    while True:
        data = generate_sensor_data()
        producer.send('sensor_data', value=data)
        print(f"Sent: {data}")
        time.sleep(1)  # Send one message per second
except KeyboardInterrupt:
    print("Stopping producer.")
finally:
    producer.close()
