from kafka import KafkaConsumer
import json
import psycopg2
from datetime import datetime

consumer = KafkaConsumer(
    "weather-events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="weather-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

conn = psycopg2.connect(
    dbname="weather",
    user="helios",
    password="",
    host="localhost",
    port="5432"
)

conn.autocommit = True
cursor = conn.cursor()

print("Weather data consumer started...")

for message in consumer:
    event = message.value
    print("Received:", event)
    #event_time = datetime.fromisoformat(event["timestamp"])
    cursor.execute("""
    INSERT INTO weather_events (CityID, City, Temperature, WindSpeed, Humidity)
    VALUES (%s, %s, %s, %s, %s)
    """, (
    event["city_id"],
    event["city"],
    event["temperature"],
    event["windSpeed"],
    event["humidity"]
    ))