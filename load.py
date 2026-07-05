# Este script extrae datos de clima y metadatos de ciudades desde OpenWeatherMap
# y los guarda en la capa bronze (Delta Lake).
#
# La extracción de metadatos es full (overwrite completo cada vez); la de clima
# es incremental, con merge/upsert por city+year+month+day+hour para no duplicar,
# particionado por año, mes, día y hora.

from extract import get_weather, get_geodata
from datetime import datetime
from deltalake import write_deltalake, DeltaTable
import pandas as pd
from deltalake.exceptions import TableNotFoundError
from config import  CITIES, MINIO_BUCKET, LAYER_BRONZE, SOURCE_SYSTEM, ENTITY_METADATA, ENTITY_WEATHER, storage_options

# ---- EXTRACCIÓN FULL ----
# para cada ciudad, consulta los metadatos geográficos y los datos climáticos, los convierte a DataFrame y los guarda en Delta Lake
metadata_path = f"s3://{MINIO_BUCKET}/{LAYER_BRONZE}/{SOURCE_SYSTEM}/{ENTITY_METADATA}"

all_city_data = []   # 1. lista vacía, ANTES del loop

for city_name, coords in CITIES.items():
    json_city_data = get_geodata(city_name)
    df_city_data = pd.json_normalize(json_city_data)
    df_city_data["city"] = city_name
    all_city_data.append(df_city_data)   # 2. acumular CADA ciudad, DENTRO del loop
    print(f"ok {city_name} metadata obtenido")

df_all_cities = pd.concat(all_city_data)   # 3. juntar TODAS las ciudades, FUERA del loop

write_deltalake(
    metadata_path,
    df_all_cities,
    mode="overwrite",
    schema_mode="merge",
    storage_options=storage_options
)
print("ok metadata completa guardada en Delta Lake")

# ---- EXTRACCIÓN INCREMENTAL ----
# para cada ciudad, consulta el clima actual, lo convierte a DataFrame, le agrega columnas de fecha y hora, y lo guarda en Delta Lake particionado por año, mes, día y hora

weather_path = f"s3://{MINIO_BUCKET}/{LAYER_BRONZE}/{SOURCE_SYSTEM}/{ENTITY_WEATHER}"

for city_name, coords in CITIES.items():
    json_weather_data = get_weather(city_name, coords["lat"], coords["lon"])
    df_weather_data = pd.json_normalize(json_weather_data)
    now = datetime.now()
    df_weather_data["city"] = city_name
    df_weather_data["year"] = now.year
    df_weather_data["month"] = now.month
    df_weather_data["day"] = now.day
    df_weather_data["hour"] = now.hour

    try:
        dt = DeltaTable(weather_path, storage_options=storage_options)
        (
            dt.merge(
                source=df_weather_data,
                predicate="source.city = target.city AND source.year = target.year AND source.month = target.month AND source.day = target.day AND source.hour = target.hour",
                source_alias="source",
                target_alias="target"
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
        print(f"ok {city_name} clima actualizado (merge) en Delta Lake")
    except TableNotFoundError:
        write_deltalake(
            weather_path,
            df_weather_data,
            mode="append",
            schema_mode="merge",
            partition_by=["year", "month", "day", "hour"],
            storage_options=storage_options
        )
        print(f"ok {city_name} clima creado por primera vez en Delta Lake")

# ---- CHEQUEO ----
dt = DeltaTable(weather_path, storage_options=storage_options)
df = dt.to_pandas()
print("Total de filas:", df.shape[0])
print("Filas duplicadas (misma city+year+month+day+hour):", df[["city", "year", "month", "day", "hour"]].duplicated().sum())

# ---- MANTENIMIENTO: limpieza de archivos obsoletos ----
dt_metadata = DeltaTable(metadata_path, storage_options=storage_options)
dt_metadata.vacuum(retention_hours=168, dry_run=False)

dt_weather = DeltaTable(weather_path, storage_options=storage_options)
dt_weather.vacuum(retention_hours=168, dry_run=False)

print("ok limpieza de archivos obsoletos (vacuum) completada")

