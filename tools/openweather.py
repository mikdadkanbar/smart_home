link="https://archive-api.open-meteo.com/v1/era5?latitude=52&longitude=4&timeformat=unixtime&start_date=2023-01-01&end_date=2023-05-15&hourly=temperature_2m,relativehumidity_2m,rain,snowfall,windspeed_10m,winddirection_10m,soil_temperature_0_to_7cm"

# No tool for the Weather data : we will just download it on demand 
# Import this file if you need the data :  'from tools.openweather import *'

import urllib.request 
import pandas as pd 
import json 

urllib.request.urlretrieve(link, "openweather_data.json")

with open('openweather_data.json') as openweather:
    openweather_json = json.load(openweather)

# Data with variables and corresponding data by time
openweather_data=pd.DataFrame(openweather_json["hourly"])

# Variables and Units related
units=pd.DataFrame(openweather_json["hourly_units"],index=[1])

# Informations about the data 
metadata_json={"latitude":openweather_json["latitude"],"longitude":openweather_json["longitude"],"generationtime_ms":openweather_json["generationtime_ms"],"utc_offset_seconds":openweather_json["utc_offset_seconds"],"timezone":openweather_json["timezone"],"elevation":openweather_json["elevation"]}
metadata=pd.DataFrame(metadata_json,index=[1])