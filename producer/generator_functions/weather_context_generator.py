"""
Weather context generator for AirPulse.

Simulates city-level meteorological context used for:
- AQI correction
- dispersion modeling
- fire risk analysis
"""

from datetime import datetime, timezone
import math
import random
from typing import Optional, Dict, Any


# ---------- helpers ----------

def _ensure_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts


def _clamp(v: float, low: float, high: float) -> float:
    return max(low, min(high, v))


# ---------- simulation ----------

def simulate_temperature(timestamp: datetime, latitude: float) -> float:
    """
    City-scale temperature (°C).
    """
    ts = _ensure_utc(timestamp)
    hour = ts.hour + ts.minute / 60.0
    day_of_year = ts.timetuple().tm_yday

    lat_factor = abs(latitude) / 90.0
    annual_avg = 22 - 12 * lat_factor

    seasonal = 10 * math.sin(2 * math.pi * (day_of_year / 365.0 - 0.25))
    diurnal = 5 * math.sin(2 * math.pi * (hour / 24.0 - 0.25))
    noise = random.gauss(0, 0.6)

    return round(_clamp(annual_avg + seasonal + diurnal + noise, -30, 55), 1)


def simulate_humidity(temperature_c: float) -> float:
    """
    Relative humidity (%), inversely related to temperature.
    """
    base = 70 - (temperature_c - 15) * 1.1
    noise = random.gauss(0, 4)
    return round(_clamp(base + noise, 10, 100), 0)


def simulate_pressure(day_of_year: int) -> int:
    """
    Surface pressure (hPa).
    """
    seasonal = 8 * math.cos(2 * math.pi * (day_of_year / 365.0))
    noise = random.gauss(0, 2)
    return int(_clamp(1013 + seasonal + noise, 980, 1045))


# ---------- top-level generator ----------

def generate_weather_context(
    city: str,
    latitude: float,
    longitude: float,
    timestamp: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    Generate weather context payload.
    """
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)
    timestamp = _ensure_utc(timestamp)

    temperature = simulate_temperature(timestamp, latitude)
    humidity = simulate_humidity(temperature)
    pressure = simulate_pressure(timestamp.timetuple().tm_yday)

    return {
        "timestamp": timestamp.isoformat(),
        "city": city,
        "temperature": temperature,
        "humidity": humidity,
        "pressure": pressure,
    }
