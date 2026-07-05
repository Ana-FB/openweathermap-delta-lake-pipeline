# Este script lee los datos crudos de clima y metadatos de ciudades guardados en la capa
# bronze (Delta Lake), aplica transformaciones de limpieza y enriquecimiento (más de las
# 4 mínimas requeridas), y guarda el resultado en la capa silver, en una única tabla
# consolidada ("clima_completo").

import pandas as pd
from deltalake import DeltaTable, write_deltalake
from config import (
    MINIO_BUCKET, LAYER_BRONZE, LAYER_SILVER, SOURCE_SYSTEM,
    ENTITY_WEATHER, ENTITY_METADATA, ENTITY_WEATHER_FULL, storage_options
)

# ---- LEER BRONZE ----
weather_path = f"s3://{MINIO_BUCKET}/{LAYER_BRONZE}/{SOURCE_SYSTEM}/{ENTITY_WEATHER}"
metadata_path = f"s3://{MINIO_BUCKET}/{LAYER_BRONZE}/{SOURCE_SYSTEM}/{ENTITY_METADATA}"

df_weather = DeltaTable(weather_path, storage_options=storage_options).to_pandas()
df_metadata = DeltaTable(metadata_path, storage_options=storage_options).to_pandas()

# ---- TRANSFORMACIONES ----
# 1. JOIN: unimos clima (incremental, por hora) con metadata de ciudad (full),
#    usando "city" como clave. how="left" para conservar todas las mediciones
#    de clima aunque falte metadata de alguna ciudad.
df_metadata_clean = df_metadata[["name", "lat", "lon", "country", "state", "city"]]

df_weather_full = df_weather.merge(df_metadata_clean, on="city", how="left")

# 2. Extracción de columna nueva por lógica: "weather" llega anidado (lista de
#    diccionarios) desde la API; extraemos "description" a una columna propia.
df_weather_full["weather_description"] = df_weather_full["weather"].apply(lambda x: x[0]["description"])

# 3. Formateo de fechas: "dt", "sys.sunrise" y "sys.sunset" llegan como timestamps
#    Unix (UTC). Se convierten a datetime y se ajustan a hora local de Argentina
#    usando el offset de la propia columna "timezone" que trae la API.

df_weather_full["dt"] = (pd.to_datetime(df_weather_full["dt"], unit="s", utc=True) + pd.to_timedelta(df_weather_full["timezone"], unit="s")).dt.tz_localize(None)
df_weather_full["sys.sunrise"] = (pd.to_datetime(df_weather_full["sys.sunrise"], unit="s", utc=True) + pd.to_timedelta(df_weather_full["timezone"], unit="s")).dt.tz_localize(None)
df_weather_full["sys.sunset"] = (pd.to_datetime(df_weather_full["sys.sunset"], unit="s", utc=True) + pd.to_timedelta(df_weather_full["timezone"], unit="s")).dt.tz_localize(None)

# 4. Columnas booleanas por lógica de negocio: umbrales elegidos pensando en un
#    invierno argentino.
df_weather_full["es_dia_caluroso"] = df_weather_full["main.temp"] > 20
df_weather_full["es_dia_helado"] = df_weather_full["main.temp"] < -1


# 5. Eliminación de columnas técnicas o redundantes: campos de uso interno de la
#    API (id, cod, base, sys.id, sys.type), coordenadas duplicadas (coord.lat/lon,
#    de la estación meteorológica más cercana, pueden no coincidir exactamente con
#    las de metadata), min/max de temperatura (redundantes para clima actual),
#    "timezone" (ya usado en el paso 3, no se necesita más) y "weather" original
#    (ya reemplazada por "weather_description").
df_weather_full = df_weather_full.drop(columns=[
    "base", "cod", "sys.id", "sys.type", "weather", "name_y", "timezone",
    "id", "coord.lon", "coord.lat", "main.temp_min", "main.temp_max", "visibility",
    "sys.country", "main.sea_level", "main.grnd_level", "wind.deg", "wind.gust"
])

# 6. Renombrado de columnas al español, para que el dataset final sea más legible.
#    "name_x" (ciudad_nombre) se descarta después del rename porque ya tenemos
#    la columna "city" propia como identificador de ciudad.
df_weather_full = df_weather_full.rename(columns={
    "dt": "fecha_hora",
    "main.temp": "temperatura",
    "main.feels_like": "sensacion_termica",
    "main.humidity": "humedad",
    "main.pressure": "presion",
    "wind.speed": "velocidad_viento",
    "name_x": "ciudad_nombre",
    "country": "pais",
    "state": "provincia",
    "sys.sunrise": "amanecer",
    "sys.sunset": "atardecer"
})
df_weather_full = df_weather_full.drop(columns=["ciudad_nombre"])

# ---- GUARDAR SILVER ----
# Estrategia: mode="overwrite" + schema_mode="overwrite". A diferencia de bronze
# (donde clima usa merge para acumular histórico sin duplicar), en silver cada
# corrida relee todo  el histórico acumulado en bronze y lo retransforma de punta
# a punta. No implica pérdida de datos: bronze ya conserva el histórico completo,
# y silver simplemente refleja ese histórico ya limpio en cada ejecución.
silver_path = f"s3://{MINIO_BUCKET}/{LAYER_SILVER}/{SOURCE_SYSTEM}/{ENTITY_WEATHER_FULL}"

write_deltalake(
    silver_path,
    df_weather_full,
    mode="overwrite",
    schema_mode="overwrite",
    storage_options=storage_options
)

print("ok clima_completo guardado en silver")