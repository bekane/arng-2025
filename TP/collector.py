"""Mini collecteur MQTT -> InfluxDB pour le TP."""

import json
import time

from influxdb_client import InfluxDBClient, Point
from paho.mqtt.client import Client

BROKER = "localhost"  # ou "broker" si vous lancez ce script dans docker compose
TOPIC = "entrepot/+/+/v1/#"
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "dev-token"
INFLUX_ORG = "ares"
BUCKET = "entrepot"

client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = client.write_api()


def on_message(mqtt_client, userdata, message):
    try:
        payload = json.loads(message.payload.decode("utf-8"))
    except json.JSONDecodeError:
        return

    print(f"Got new message !! {payload}")

    zone = payload.get("zone", "unknown")
    sensor = payload.get("sensor", "thermo")
    point = (
        Point("telemetrie")
        .tag("zone", zone)
        .tag("sensor", sensor)
        .field("temp", float(payload.get("temp", 0.0)))
        .field("hum", float(payload.get("hum", 0.0)))
        .field("alert_level", payload.get("alert_level", "normal"))
        .time(time.time_ns())
    )

    # Ici, implémentez votre statégie des doublons.
    # Vous pouvez littéralement indiquer l'algorithme en commentaire

    write_api.write(bucket=BUCKET, org=INFLUX_ORG, record=point)

    print(f"Message stored on {BUCKET}")


def main():
    mqtt = Client(client_id="tp-collector")
    mqtt.on_message = on_message
    mqtt.connect(BROKER, 1883, keepalive=60)
    mqtt.subscribe(TOPIC, qos=1)
    mqtt.loop_forever()


if __name__ == "__main__":
    main()
