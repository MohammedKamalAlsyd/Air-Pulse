from datetime import datetime, timedelta
from typing import Sequence, Dict, Optional

from producer.base_producer.base_producer import BaseProducer
from producer.generator_functions.fire_alarm_generator import generate_fire_alarm


class AlarmProducer(BaseProducer):
    """
    Produces rare fire alarm events.
    """

    def __init__(
        self,
        sensor_ids: Sequence[str],
        locations: Dict[str, tuple[float, float]],
        kafka_conf: Optional[dict] = None,
        topic: str = "alerts.fire",
    ):
        super().__init__(kafka_conf)
        self.sensor_ids = sensor_ids
        self.locations = locations
        self.topic = topic

    def run(
        self,
        start_time: datetime,
        delta: timedelta,
        iterations: int,
    ) -> None:
        for i in range(iterations):
            ts = start_time + i * delta

            for sensor_id in self.sensor_ids:
                event = generate_fire_alarm(
                    sensor_id=sensor_id,
                    location=self.locations.get(sensor_id),
                    timestamp=ts,
                )
                if event:
                    self.produce(self.topic, event, key=sensor_id)