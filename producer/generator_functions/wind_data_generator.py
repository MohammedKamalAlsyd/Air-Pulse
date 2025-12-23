"""
Wind data generator for AirPulse.

Produces realistic wind speed and direction with persistence
(previous state influences next state).
"""

from datetime import datetime, timezone
import random
from typing import Optional, Dict, Any
from utils.main import ensure_utc, clamp


# ---------- helpers ----------

DIRECTIONS = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]

def _direction_from_degrees(deg: float) -> str:
    idx = int((deg + 22.5) // 45) % 8
    return DIRECTIONS[idx]


# ---------- simulation ----------

def simulate_wind(
    prev_speed: float = 3.5,
    prev_direction_deg: float = 0.0,
) -> tuple[float, float]:
    """
    Simulate wind with temporal smoothness.
    """
    speed_change = random.gauss(0, 0.6)
    direction_change = random.gauss(0, 15)

    speed = clamp(prev_speed + speed_change, 0.0, 30.0)
    direction = (prev_direction_deg + direction_change) % 360

    return round(speed, 1), round(direction, 1)


# ---------- top-level generator ----------

def generate_wind_data(
    city: str,
    prev_speed: float = 3.5,
    prev_direction_deg: float = 0.0,
    timestamp: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    Generate wind payload.
    """
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)
    timestamp = ensure_utc(timestamp)

    speed, direction_deg = simulate_wind(prev_speed, prev_direction_deg)

    return {
        "timestamp": timestamp.isoformat(),
        "city": city,
        "wind_speed": speed,
        "wind_direction": _direction_from_degrees(direction_deg),
        "wind_direction_degrees": direction_deg,
    }
