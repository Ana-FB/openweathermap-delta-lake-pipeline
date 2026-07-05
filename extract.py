# Este script extrae datos de clima actual y metadatos de ciudades desde la
# API de OpenWeatherMap, y los almacena en formato Delta Lake.
#
# La extracción de metadatos es full (se trae todo cada vez), mientras que
# la extracción de clima es incremental, con partición por año, mes, día y hora.
import pandas as pd
import requests
from pprint import pprint
from config import CITIES, API_KEY

# ---- FUNCIONES ----
def get_weather(city_name, lat, lon):
    # consulta el clima actual de una ciudad al endpoint /data/2.5/weather
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "lat": lat,
        "lon": lon,
        "lang": "es",       # respuesta en español
        "appid": API_KEY,
        "units": "metric"   # temperatura en Celsius
    }
    response = requests.get(url, params=params) 
    try:
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"La petición falló para {city_name}: {e}")
        return None
    
def get_geodata(city_name):
    # consulta los metadatos geográficos de una ciudad al endpoint /geo/1.0/direct
    url = "https://api.openweathermap.org/geo/1.0/direct"
    params = {
        "q": f"{city_name},AR",  # busca la ciudad en Argentina
        "limit": 1,              # trae solo el primer resultado
        "appid": API_KEY
    }
    response = requests.get(url, params=params)
    try:
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"La petición falló para {city_name}: {e}")
        return None

def build_table(json_data):
    # convierte una lista de respuestas JSON en un DataFrame de pandas
    try:
        df = pd.json_normalize(json_data)
        return df
    except Exception as e:
        print(f"Los datos no están en el formato esperado: {e}")
        return None
