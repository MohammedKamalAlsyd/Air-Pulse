# utils/main.py
import uuid
from datetime import datetime, timezone

def get_uuid_str() -> str:
    """Return a compact UUID4 string (hex)."""
    return uuid.uuid4().hex


def ensure_utc(ts: datetime) -> datetime:
    """Return ISO formatted UTC timestamp string for consistent serialization."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


def time_features(ts: datetime):
    """Return hour_of_day (0-24 float), day_of_year (1-366), and seconds_since_midnight."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    hour = ts.hour + ts.minute / 60.0 + ts.second / 3600.0
    day_of_year = ts.timetuple().tm_yday
    seconds_since_midnight = ts.hour * 3600 + ts.minute * 60 + ts.second
    return hour, day_of_year, seconds_since_midnight

def clamp(v: float, low: float, high: float) -> float:
    return max(low, min(high, v))