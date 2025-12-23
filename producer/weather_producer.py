from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any

from producer.base_producer.base_producer import BaseProducer
from producer.generator_functions.weather_context_generator import generate_weather_context
from producer.generator_functions.wind_data_generator import generate_wind_data
from producer.generator_functions.air_quality_generator import generate_air_quality


class WeatherProducer(BaseProducer):
    """
    Produces:
    - weather context
    - wind data
    - air quality (city-level)
    """

    def __init__(
        self,
        cities: List[Dict[str, Any]],
        kafka_conf: Optional[dict] = None,
        weather_topic: str = "weather.context",
        wind_topic: str = "weather.wind",
        air_quality_topic: str = "air.quality",
    ):
        super().__init__(kafka_conf)
        self.cities = cities
        self.weather_topic = weather_topic
        self.wind_topic = wind_topic
        self.air_quality_topic = air_quality_topic

        # Stateful wind tracking per city
        self._wind_state = {
            c["city"]: {"speed": 3.0, "dir": 0.0} for c in cities
        }

    def run(
        self,
        start_time: datetime,
        delta: timedelta,
        iterations: int,
    ) -> None:
        for i in range(iterations):
            ts = start_time + i * delta

            for c in self.cities:
                city = c["city"]
                lat = c["lat"]
                lon = c["lon"]

                weather = generate_weather_context(
                    city=city,
                    latitude=lat,
                    longitude=lon,
                    timestamp=ts,
                )
                self.produce(self.weather_topic, weather, key=city)

                prev = self._wind_state[city]
                wind = generate_wind_data(
                    city=city,
                    prev_speed=prev["speed"],
                    prev_direction_deg=prev["dir"],
                    timestamp=ts,
                )
                self._wind_state[city] = {
                    "speed": wind["wind_speed"],
                    "dir": wind["wind_direction_degrees"],
                }
                self.produce(self.wind_topic, wind, key=city)

                air_quality = generate_air_quality(
                    sensor_id=f"{city}-AQ",
                    timestamp=ts,
                )
                self.produce(self.air_quality_topic, air_quality, key=city)
