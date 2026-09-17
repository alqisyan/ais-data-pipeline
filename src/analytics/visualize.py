#Runable on Jupyter Notebook only
import requests
import json
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

#Global Variable
positions = []
speed = []
country = []
allShipData = []

# Connect to FastAPI
url = "http://localhost:8000/ships"
response = requests.get(url)

# Data Parsing based on type
if response.status_code == 200:
    payload = response.json()
    for i in payload:
        data = payload.get(i)
        if len(data) == 2:
            print("Skipp")
        else:
            nested_data = data.get("mainCalculator", {})
            lat = nested_data.get("lat", 0)
            lon = nested_data.get("lon", 0)
            sog = nested_data.get("sog", 0)
            flag = data.get("country", {})
            positions.append({"ship_id": i, "lat": lat, "lon": lon})
            speed.append({"ship_id": i, "speed": sog})
            country.append({"ship_id": i, "Country": flag})
        
else:
    print(f"Failed to fetch data. Status code: {response.status_code}")

#Create Scatter plot of Ship Positions
df = pd.DataFrame(positions)
plt.ion()
fig, ax = plt.subplots()
scat = ax.scatter(df["lon"], df["lat"], c="blue", alpha=0.6)
ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")
ax.set_xlim(109.1, 109.9)
ax.set_ylim(-0.01, 0.05)
ax.set_title("Distribution of AIS Ship Positions")
plt.show()

#Create a Pie diagram of Vessel speed
x = pd.DataFrame(speed)
speed_counts = x["speed"].value_counts()
plt.figure(figsize=(7,7))
plt.pie(
    speed_counts,
    labels=speed_counts.index,
    autopct="%1.1f%%",
    startangle=90
)
plt.title("Distribution of Vessel Speeds (Knot)")
plt.show()

#Create a chart of Vessel Flag by AIS
y = pd.DataFrame(country)
flags = y["Country"].value_counts()
plt.figure(figsize=(7,7))
flags.plot(kind="bar", color="skyblue")
plt.title("Number of Ships Detected by AIS per Country")
plt.xlabel("Country")
plt.ylabel("Number of Ships")
plt.show()

#Saved Ships Data on csv
#df.to_csv("ais_data.csv", index=False, columns=["ship_id", "lat", "lon"], header=True)