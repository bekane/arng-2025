"""
Point d'entrée container API.

Exporte un objet FastAPI nommé `app` utilisé par Uvicorn.
Les variables d'environnement suivantes pilotent la connexion InfluxDB :
  - INFLUX_URL (ex: http://influxdb:8086)
  - INFLUX_TOKEN
  - INFLUX_ORG
  - INFLUX_BUCKET
"""

import os

from tp_mqtt import create_app


def _env(name: str, *, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Missing env var {name}")
    return value


app = create_app(
    influx_url=_env("INFLUX_URL", default="http://influxdb:8086"),
    token=_env("INFLUX_TOKEN", default="dev-token"),
    org=_env("INFLUX_ORG", default="ares"),
    bucket=_env("INFLUX_BUCKET", default="entrepot"),
)
