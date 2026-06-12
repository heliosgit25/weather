import json
import time
import requests
from datetime import datetime
from kafka import KafkaProducer

city = input("Enter city name: ")
api_key = "742617a6ce5216e5cbc036849c036c3a"

url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
response = requests.get(url)
resp_code = response.status_code
print(resp_code)
data = response.json()
#print(data.keys())
#print(data)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def weatherEvent():
    return {
    "city_id": data["id"],
    "city": data["name"],
    "temperature": data["main"]["temp"],
    "windSpeed": data["wind"]["speed"],
    "humidity": data["main"]["humidity"]
}

if __name__ == "__main__":
    try:
        while True:
            event = weatherEvent()
            producer.send("weather-events", event)
            print("\nEvent sent.\n")
            print(f"Weather Data for {event['city']}\n", event)
            time.sleep(5)
    except KeyboardInterrupt:
        print("\nWeather event generator stopping...")