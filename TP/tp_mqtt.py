"""
TP ARNG — Chaîne MQTT -> InfluxDB
================================

Ce fichier regroupe les squelettes minimum attendus pendant le TP :

1. Aider à générer/configurer Mosquitto.
2. Simuler un capteur MQTT en Python.
3. Ébaucher l'API FastAPI branchée sur InfluxDB.
4. Centraliser quelques commandes/tests utiles.

Complétez chaque fonction selon votre contexte (adresse broker, bucket, secrets, etc.).
"""

from __future__ import annotations  



from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


# --------------------------------------------------------------------------- #
# Activité 1 — Configuration Mosquitto                                       #
# --------------------------------------------------------------------------- #

MOSQUITTO_CONFIG_TEMPLATE = """\
listener {port} 0.0.0.0
persistence true
autosave_interval 60
allow_anonymous {allow_anonymous}
persistence_location /mosquitto/data

# Optimisations TP
max_inflight_messages 20
retry_interval 30
"""


def write_sample_mosquitto_config(
    destination: Path,
    *,
    port: int = 1883,
    allow_anonymous: bool = True,
) -> None:
    """
    Génère un fichier de configuration Mosquitto basique prêt à être adapté.
    """

    destination.write_text(
        MOSQUITTO_CONFIG_TEMPLATE.format(
            port=port,
            allow_anonymous=str(allow_anonymous).lower(),
        ),
        encoding="utf-8",
    )


def broker_smoke_commands(host: str = "localhost") -> list[str]:
    """
    Retourne deux commandes `mosquitto_pub` / `mosquitto_sub` pour vérifier le broker.
    """

    pub = (
        'mosquitto_pub -h {host} -t "entrepot/nord/temp/v1" '
        "-m '{{\"temp\":23.1,\"hum\":44.0}}' -q 1"
    ).format(host=host)
    sub = 'mosquitto_sub -h {host} -t "entrepot/#" -v'.format(host=host)
    return [pub, sub]


# --------------------------------------------------------------------------- #
# Activité 2 — Capteur simulé (publication MQTT)                             #
# --------------------------------------------------------------------------- #

try:
    from paho.mqtt.client import Client  # type: ignore
except ImportError:  # pragma: no cover - laissé au binôme de gérer l'env
    Client = None  # type: ignore


@dataclass
class SensorConfig:
    broker_host: str = "localhost"
    broker_port: int = 1883
    topic_pattern: str = "entrepot/{zone}/{sensor}/v1"
    zones: tuple[str, ...] = ("nord", "sud", "centre")
    sensor_name: str = "thermo"
    qos: int = 1
    period_seconds: int = 5


class SensorSimulator:
    """
    Simule un capteur température/humidité. Remplissez `compute_payload`.
    """

    def __init__(self, config: SensorConfig):
        if Client is None:
            raise RuntimeError("paho-mqtt manquant : pip install paho-mqtt")
        self.config = config
        self.client = Client(client_id="simu-capteur")

    def connect(self) -> None:
        self.client.connect(self.config.broker_host, self.config.broker_port, keepalive=60)

    def compute_payload(self, zone: str) -> dict:
        """
        Retourne le payload JSON publié sur MQTT.
        TODO: injecter timestamp, mesures aléatoires/cohérentes, alert_level, version.
        """

        return {
            "timestamp": 0.0,
            "zone": zone,
            "temp": 0.0,
            "hum": 0.0,
            "alert_level": "normal",
            "version": 1,
        }

    def publish_once(self) -> None:
        import json
        import time

        for zone in self.config.zones:
            topic = self.config.topic_pattern.format(zone=zone, sensor=self.config.sensor_name)
            payload = json.dumps(self.compute_payload(zone))
            self.client.publish(topic, payload, qos=self.config.qos, retain=False)
        time.sleep(self.config.period_seconds)


# --------------------------------------------------------------------------- #
# Activité 3 — API REST Python + InfluxDB                                     #
# --------------------------------------------------------------------------- #

try:
    from fastapi import FastAPI, HTTPException  # type: ignore
    from pydantic import BaseModel  # type: ignore
    from influxdb_client import InfluxDBClient, Point  # type: ignore
except ImportError:  # pragma: no cover - à installer selon les besoins
    FastAPI = None  # type: ignore
    HTTPException = None  # type: ignore
    BaseModel = object  # type: ignore
    InfluxDBClient = None  # type: ignore
    Point = None  # type: ignore


class Threshold(BaseModel):  # type: ignore[misc]
    zone: str
    temp_high: float
    humidity_high: float | None = None


class Alert(BaseModel):  # type: ignore[misc]
    zone: str
    message: str
    threshold: float


def create_app(
    *,
    influx_url: str = "http://localhost:8086",
    token: str = "TOKEN",
    org: str = "ORG",
    bucket: str = "entrepot",
):
    """
    Construit l'application FastAPI. Complétez la logique métier selon vos besoins.
    """

    if FastAPI is None:
        raise RuntimeError("FastAPI / influxdb-client non installés.")

    app = FastAPI(title="TP MQTT API")
    client = InfluxDBClient(url=influx_url, token=token, org=org)
    query_api = client.query_api()
    write_api = client.write_api()

    @app.get("/zones/{zone}/latest")
    def latest_measure(zone: str):
        query = f"""
from(bucket: "{bucket}")
  |> range(start: -5m)
  |> filter(fn: (r) => r["_measurement"] == "telemetrie" and r["zone"] == "{zone}")
  |> last()
"""
        tables = query_api.query(query)
        if not tables:
            raise HTTPException(status_code=404, detail="No data")  # type: ignore[arg-type]
        return tables[0].records[-1].values

    @app.post("/alerts")
    def create_alert(alert: Alert):
        point = (
            Point("alerts")
            .tag("zone", alert.zone)
            .field("threshold", alert.threshold)
            .field("message", alert.message)
        )
        write_api.write(bucket=bucket, org=org, record=point)
        return {"status": "stored"}

    @app.get("/thresholds")
    def list_thresholds() -> Iterable[Threshold]:
        """
        TODO: relier à une base de config ou à InfluxDB si vous stockez les seuils dans un bucket dédié.
        """
        # 1 . definir la query flux 


        # 2. executer la query

        # 3. construire les objets Threshold et retourner la liste


        return [Threshold(zone="nord", temp_high=26.5)]

    return app

# --------------------------------------------------------------------------- #
# Activité 4 — Tests manuels & supervision                                    #
# --------------------------------------------------------------------------- #

DASHBOARD_FLUX_QUERY = """\
from(bucket: "entrepot")
  |> range(start: -1h)
  |> filter(fn: (r) => r["_measurement"] == "telemetrie")
  |> aggregateWindow(every: 1m, fn: mean)
"""


def curl_examples(api_base: str = "http://localhost:8000") -> list[str]:
    """
    Commandes curl à documenter pendant le TP.
    """

    latest = f"curl {api_base}/zones/nord/latest"
    create_alert = (
        f"curl -X POST {api_base}/alerts "
        '-H "Content-Type: application/json" '
        "-d '{\"zone\":\"nord\",\"message\":\"maintenance\",\"threshold\":26.5}'"
    )
    thresholds = f"curl {api_base}/thresholds"
    return [latest, create_alert, thresholds]


if __name__ == "__main__":
    import argparse
    import textwrap

    parser = argparse.ArgumentParser(description="Helpers pour le TP MQTT → InfluxDB")
    parser.add_argument(
        "action",
        choices=["config", "sensor", "api", "curl"],
        help="Quel composant initialiser (affiche uniquement un squelette).",
    )
    parser.add_argument("--output", type=Path, help="Chemin du fichier de config Mosquitto.")
    args = parser.parse_args()

    if args.action == "config":
        destination = args.output or Path("mosquitto.conf")
        write_sample_mosquitto_config(destination)
        print(f"Configuration exemple écrite dans {destination}")
        print("\nTests rapides :")
        for cmd in broker_smoke_commands():
            print("  ", cmd)
    elif args.action == "sensor":
        print(
            textwrap.dedent(
                """
                Instancier SensorSimulator(config) puis appeler:

                    simulator.connect()
                    while True:
                        simulator.publish_once()

                Complétez compute_payload pour générer des mesures réalistes.
                """
            ).strip()
        )
    elif args.action == "api":
        print(
            textwrap.dedent(
                """
                from tp_mqtt import create_app
                app = create_app(token="XXX", org="YYY", bucket="entrepot")

                uvicorn.run(app, host="0.0.0.0", port=8000)
                """
            ).strip()
        )
    elif args.action == "curl":
        print("Commandes à documenter :")
        for cmd in curl_examples():
            print("  ", cmd)
