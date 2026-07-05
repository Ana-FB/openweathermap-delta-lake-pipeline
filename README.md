# Pipeline OpenWeatherMap → Delta Lake (Bronze/Silver)

Trabajo integrador de Data Engineering (UTN Buenos Aires) — pipeline que extrae datos climáticos de OpenWeatherMap, los almacena en formato Delta Lake sobre MinIO, y los transforma siguiendo una arquitectura medallion (bronze/silver).

## Arquitectura

```
OpenWeatherMap API
      │
      ▼
extract.py  ──►  load.py  ──►  Bronze (Delta Lake / MinIO)
                                       │
                                       ▼
                              transform.py
                                       │
                                       ▼
                              Silver (Delta Lake / MinIO)
                              tabla: clima_completo
```

## Estructura del proyecto

| Archivo | Responsabilidad |
|---|---|
| `config.py` | Variables de configuración: API key, ciudades, paths de Delta Lake, credenciales de MinIO |
| `extract.py` | Funciones de conexión a la API: clima actual (`get_weather`) y metadatos geográficos (`get_geodata`) |
| `load.py` | Extracción full (metadata) e incremental (clima) → escritura en capa bronze |
| `transform.py` | Lectura de bronze, transformaciones de limpieza/enriquecimiento → escritura en capa silver |

## Extracción de datos

- **Full** (`Localizacion`): metadatos geográficos de cada ciudad (lat/lon, país, provincia). Se sobrescribe completo en cada corrida (`mode="overwrite"`), porque son datos que no cambian con el tiempo.
- **Incremental** (`Clima`): clima actual de cada ciudad, consultado periódicamente. Se usa `merge`/upsert por `city + year + month + day + hour` para no duplicar mediciones, con partición física por año/mes/día/hora.

## Capa Bronze

Datos crudos tal como llegan de la API, con:
- Columna `city` agregada explícitamente en ambas entidades (clave para el join en silver).
- Columnas de partición (`year`, `month`, `day`, `hour`) en la entidad de clima.
- Patrón `try/except TableNotFoundError`: primera corrida crea la tabla particionada, corridas siguientes hacen merge/upsert.
- `vacuum(retention_hours=168)` para limpieza de archivos obsoletos.

## Capa Silver

Tabla consolidada `clima_completo`, resultado de aplicar sobre bronze:

1. **JOIN** entre clima y metadata por `city` (`how="left"`, para no perder mediciones si falta metadata de alguna ciudad).
2. **Extracción de columna anidada**: `weather_description` desde el campo `weather` (lista de diccionarios de la API).
3. **Conversión de timestamps**: `dt`, `sys.sunrise` y `sys.sunset` (Unix/UTC) a hora local de Argentina, usando el offset de la columna `timezone`.
4. **Columnas booleanas de negocio**: `es_dia_caluroso` (>20°C) y `es_dia_helado` (<-1°C).
5. **Eliminación de columnas técnicas/redundantes**: campos internos de la API, coordenadas duplicadas, min/max de temperatura, `timezone` y `weather` original.
6. **Renombrado al español** de las columnas finales.

**Estrategia de guardado:** `mode="overwrite"` + `schema_mode="overwrite"`. A diferencia de bronze (que acumula histórico sin duplicar vía merge), silver relee todo el histórico de bronze y lo retransforma en cada corrida — el overwrite no implica pérdida de datos, ya que bronze conserva el histórico completo.

## Stack

Python · pandas · deltalake (delta-rs) · MinIO (S3-compatible storage) · python-dotenv

## Cómo correr

```bash
python load.py       # extracción + escritura en bronze
python transform.py  # lectura de bronze + transformación + escritura en silver
```

Requiere un archivo `.env` (no incluido en el repo) con las credenciales de la API y de MinIO — ver variables usadas en `config.py`.
