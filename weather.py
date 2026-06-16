import os
import json
import time
import requests
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

city = input("Enter city name: ")
api_key = os.getenv("API_KEY")
#print(api_key)

url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
#response = requests.get(url)
#resp_code = response.status_code
#print(resp_code)
#data = response.json()
#print(data.keys())
#print(data)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def weatherEvent(data):
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
            response = requests.get(url)
            code = response.status_code
            if (code == 200):
                data = response.json()
            elif (response.status_code == 401):
                print("API Key Error, exiting... ")
                break
            else:
                print("Temporary error: ", code)
            event = weatherEvent(data)
            producer.send("weather-events", event)
            print("\nEvent sent.\n")
            print(f"Weather Data for {event['city']}\n", event)
            time.sleep(5)
    except KeyboardInterrupt:
        print("\nWeather event generator stopping...")