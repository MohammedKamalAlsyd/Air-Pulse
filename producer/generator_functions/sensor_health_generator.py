"""
Sensor health generator for AirPulse.

Simulates operational metrics of environmental sensors:
- battery level (slow drain + occasional recharge)
- signal strength (RSSI-like, dBm)
- health status transitions (OK / DEGRADED / OFFLINE)

Designed for:
- streaming joins with sensor_measurements
- alerting pipelines
- stateful Spark processing
"""

from datetime import datetime, timezone
import random
from typing import Optional, Dict, Any


# ---------- helpers ----------

def _clamp(v: float, low: float, high: float) -> float:
    return max(low, min(high, v))


def _ensure_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts


# ---------- simulation functions ----------

def simulate_battery_level(
    previous_level: Optional[int] = None
) -> int:
    """
    Simulate battery level (%).

    - Slowly decreases over time
    - Occasional recharge / replacement events
    """
    if previous_level is None:
        previous_level = random.randint(70, 100)

    # normal drain
    drain = random.uniform(0.1, 0.6)

    # occasional recharge / maintenance
    if random.random() < 0.01:
        return random.randint(90, 100)

    value = previous_level - drain
    return int(_clamp(value, 0, 100))


def simulate_signal_strength() -> int:
    """
    Simulate signal strength (dBm).

    Typical IoT ranges:
    -30  excellent
    -60  good
    -80  weak
    -100 unusable
    """
    base = random.gauss(-65, 8)

    # occasional interference
    if random.random() < 0.03:
        base -= random.uniform(10, 25)

    return int(_clamp(base, -120, -30))


def determine_status(
    battery_level: int,
    signal_strength: int
) -> str:
    """
    Determine sensor health status based on metrics.
    """
    if battery_level <= 5:
        return "OFFLINE"

    if battery_level < 20 or signal_strength < -85:
        return "DEGRADED"

    return "OK"


# ---------- top-level generator ----------

def generate_sensor_health(
    sensor_id: str,
    sensor_type: str = "environmental",
    model: str = "AQ-2024",
    manufacturer: str = "EnviroSensors Inc.",
    timestamp: Optional[datetime] = None,
    previous_battery_level: Optional[int] = None
) -> Dict[str, Any]:
    """
    Generate a sensor health payload.

    Parameters
    ----------
    sensor_id : str
        Unique sensor identifier
    sensor_type : str
        Category of sensor (environmental, traffic, weather, etc.)
    model : str
        Hardware model
    manufacturer : str
        Manufacturer name
    timestamp : datetime, optional
        Event time (UTC). Defaults to now.
    previous_battery_level : int, optional
        Allows stateful battery simulation across events

    Returns
    -------
    dict
        JSON-serializable sensor health record
    """
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)
    timestamp = _ensure_utc(timestamp)

    battery_level = simulate_battery_level(previous_battery_level)
    signal_strength = simulate_signal_strength()
    status = determine_status(battery_level, signal_strength)

    payload = {
        "sensor_id": sensor_id,
        "timestamp": timestamp.isoformat(),
        "battery_level": battery_level,
        "signal_strength": signal_strength,
        "status": status,
        "sensor_type": sensor_type,
        "model": model,
        "manufacturer": manufacturer,
    }

    return payload
