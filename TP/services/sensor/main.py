"""
Boucle de publication MQTT exécutée dans le conteneur `sensor`.
Complétez `tp_mqtt.SensorSimulator.compute_payload` pour personnaliser les mesures.
"""

import logging
import time

from tp_mqtt import SensorConfig, SensorSimulator


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger(__name__)


def run_forever() -> None:
    simulator = SensorSimulator(
        SensorConfig(
            broker_host="broker",
            broker_port=1883,
            sensor_name="thermo",
        )
    )
    # Possibilité de créer un capteur par zone si besoin
    # 
    simulator.connect()
    LOGGER.info("Connecté au broker, démarrage des publications...")
    while True:
        simulator.publish_once()
        LOGGER.debug("Batch publié, pause %ss", simulator.config.period_seconds)
        time.sleep(0)


if __name__ == "__main__":
    run_forever()
