from kafka import KafkaConsumer
import sqlite3
import json

consumer = KafkaConsumer(
    'sensor_data',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='sensor_group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

def setup_database():
    conn = sqlite3.connect("C:\\Users\\Usuario\\Desktop\\sqlite-tools-win-x64-3470200\\sensor_data.db")
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sensor_readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_id INTEGER,
            temperature REAL,
            humidity REAL,
            timestamp REAL
        )
    ''')
    conn.commit()
    return conn

conn = setup_database()
cursor = conn.cursor()

try:
    print("Starting consumer...")
    for message in consumer:
        data = message.value
        print(f"Received: {data}")
        cursor.execute('''
            INSERT INTO sensor_readings (sensor_id, temperature, humidity, timestamp)
            VALUES (?, ?, ?, ?)
        ''', (data['sensor_id'], data['temperature'], data['humidity'], data['timestamp']))
        conn.commit()
except KeyboardInterrupt:
    print("Stopping consumer.")
finally:
    conn.close()
    consumer.close()
