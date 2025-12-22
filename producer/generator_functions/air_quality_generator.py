"""
Air Quality Index (AQI) generator for AirPulse.

Simulates AQI values and EPA-style categories based on time patterns.
Designed to align with PM2.5 / NO2 behavior from sensor_measurement_data.
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


# ---------- AQI logic ----------

AQI_CATEGORIES = [
    (0, 50, "Good"),
    (51, 100, "Moderate"),
    (101, 150, "Unhealthy for Sensitive Groups"),
    (151, 200, "Unhealthy"),
    (201, 300, "Very Unhealthy"),
    (301, 500, "Hazardous"),
]


def _aqi_category(aqi: int) -> str:
    for low, high, label in AQI_CATEGORIES:
        if low <= aqi <= high:
            return label
    return "Hazardous"


def simulate_aqi(timestamp: datetime) -> int:
    """
    Simulate AQI with diurnal + seasonal patterns.

    - Morning & evening traffic peaks
    - Seasonal winter pollution bump
    """
    ts = _ensure_utc(timestamp)
    hour = ts.hour + ts.minute / 60.0
    day_of_year = ts.timetuple().tm_yday

    # seasonal baseline (winter worse)
    seasonal = 45 + 25 * math.cos(2 * math.pi * (day_of_year / 365.0))

    # traffic-related peaks
    morning = 55 * math.exp(-0.5 * ((hour - 8) / 1.8) ** 2)
    evening = 65 * math.exp(-0.5 * ((hour - 18) / 2.0) ** 2)

    noise = random.gauss(0, 8)

    # occasional pollution episode
    event = 0.0
    if random.random() < 0.02:
        event = random.uniform(40, 120)

    value = seasonal + morning + evening + noise + event
    return int(_clamp(value, 0, 500))


# ---------- top-level generator ----------

def generate_air_quality(
    sensor_id: str,
    timestamp: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Generate an AQI record.

    Returns a JSON-serializable dict.
    """
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)
    timestamp = _ensure_utc(timestamp)

    aqi = simulate_aqi(timestamp)

    return {
        "sensor_id": sensor_id,
        "timestamp": timestamp.isoformat(),
        "aqi": aqi,
        "category": _aqi_category(aqi),
    }
