from datetime import datetime, timedelta
from typing import Dict, Sequence, Optional

from producer.base_producer.base_producer import BaseProducer
from producer.generator_functions.sensor_measurement_data import generate_sensor_date
from producer.generator_functions.sensor_health_generator import generate_sensor_health


class SensorProducer(BaseProducer):
    """
    Produces:
    - sensor measurement data
    - sensor health data
    """

    def __init__(
        self,
        sensor_ids: Sequence[str],
        locations: Dict[str, tuple[float, float]],
        kafka_conf: Optional[dict] = None,
        measurement_topic: str = "sensor.measurements",
        health_topic: str = "sensor.health",
    ):
        super().__init__(kafka_conf)
        self.sensor_ids = sensor_ids
        self.locations = locations
        self.measurement_topic = measurement_topic
        self.health_topic = health_topic

        # Battery level per sensor
        self._battery_state: Dict[str, int] = {}

    def run(
        self,
        start_time: datetime,
        delta: timedelta,
        iterations: int,
    ) -> None:
        for i in range(iterations):
            ts = start_time + i * delta

            for sensor_id in self.sensor_ids:
                location = self.locations.get(sensor_id, (0.0, 0.0))

                measurement = generate_sensor_date(
                    sensor_id=sensor_id,
                    location=location,
                    timestamp=ts,
                )
                self.produce(self.measurement_topic, measurement, key=sensor_id)

                health = generate_sensor_health(
                    sensor_id=sensor_id,
                    timestamp=ts,
                    previous_battery_level=self._battery_state.get(sensor_id),
                )
                self._battery_state[sensor_id] = health["battery_level"]
                self.produce(self.health_topic, health, key=sensor_id)

        self.flush()
