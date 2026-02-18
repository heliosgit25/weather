import requests

api_key = "2b51877d3f08bc687f1e2c8dc70fd39f"
city = input("Enter city name: ")

url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
response = requests.get(url)

data = response.json()
#print(data.keys())
#print(data)

temp = data["main"]["temp"]
feels = data["main"]["feels_like"]
print(f"Temperature:, {temp}C, But it feels like:, {feels}C")