import os
from dotenv import load_dotenv

load_dotenv()

# ---- API ----
API_KEY = os.getenv("API_KEY")

CITIES = {
    "buenos_aires": {"lat": -34.6037, "lon": -58.3816},
    "cordoba": {"lat": -31.4135, "lon": -64.1811},
    "rosario": {"lat": -32.9442, "lon": -60.6505}
}

# ---- MinIO / Delta Lake ----
MINIO_BUCKET = os.getenv("AWS_BUCKET_NAME")
LAYER_BRONZE = "bronze"
LAYER_SILVER = "silver"
SOURCE_SYSTEM = "openweathermap"
ENTITY_WEATHER = "Clima"
ENTITY_METADATA = "Localizacion"
ENTITY_WEATHER_FULL = "clima_completo"  

storage_options = {
    "AWS_ENDPOINT_URL": os.getenv("AWS_ENDPOINT_URL"),
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "AWS_ALLOW_HTTP": os.getenv("AWS_ALLOW_HTTP"),
    "AWS_CONDITIONAL_PUT": os.getenv("AWS_CONDITIONAL_PUT"),
    "AWS_S3_ALLOW_UNSAFE_RENAME": os.getenv("AWS_S3_ALLOW_UNSAFE_RENAME")
}