"""
Fire alarm event generator for AirPulse.

Produces low-frequency, high-impact fire alarm events.
Designed for alerting, SLA testing, and anomaly pipelines.
"""

from datetime import datetime, timezone
import random
import uuid
from typing import Optional, Dict, Any
from utils.main import ensure_utc


# ---------- simulation ----------

def simulate_fire_probability() -> bool:
    """
    Determine whether a fire event occurs.

    - Very rare under normal conditions
    - Probability spikes simulate real-world incidents
    """
    base_probability = 0.0005  # normal background risk

    # occasional high-risk window
    if random.random() < 0.01:
        return random.random() < 0.1

    return random.random() < base_probability


# ---------- top-level generator ----------

def generate_fire_alarm(
    sensor_id: str,
    location: Optional[tuple[float, float]] = None,
    timestamp: Optional[datetime] = None
) -> Optional[Dict[str, Any]]:
    """
    Generate a fire alarm event.

    Returns:
    - dict if alarm triggered
    - None if no event occurred (most of the time)
    """
    if not simulate_fire_probability():
        return None

    if timestamp is None:
        timestamp = datetime.now(timezone.utc)
    timestamp = ensure_utc(timestamp)

    return {
        "event_id": str(uuid.uuid4()),
        "sensor_id": sensor_id,
        "timestamp": timestamp.isoformat(),
        "alarm_type": "FIRE",
        "severity": random.choice(["LOW", "MEDIUM", "HIGH", "CRITICAL"]),
        "location": location,
        "acknowledged": False,
    }
