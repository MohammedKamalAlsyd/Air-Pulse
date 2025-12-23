from __future__ import annotations
from datetime import datetime, timezone
from typing import Tuple, Any

def ensure_utc(ts: datetime) -> datetime:
    """
    Ensure datetime is timezone-aware and in UTC.
    Raises a ValueError if ts is None or not a datetime.
    """
    if ts is None:
        raise ValueError("timestamp must not be None")
    if not isinstance(ts, datetime):
        raise TypeError(f"expected datetime, got {type(ts)}")
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def clamp(value: Any, low: float, high: float) -> float:
    """
    Clamp value into [low, high].
    Accepts anything coercible to float and raises a helpful error otherwise.
    """
    if value is None:
        raise TypeError("clamp() received None where a number was expected")
    try:
        v = float(value)
    except (TypeError, ValueError) as exc:
        raise TypeError(f"clamp() expected a numeric value, got {type(value)}") from exc
    if low is None or high is None:
        raise ValueError("clamp() low and high must be provided and not None")
    return max(float(low), min(float(high), v))


def time_features(timestamp: datetime) -> Tuple[float, int, int]:
    """
    Return (hour_decimal, day_of_year, weekday) for the given timestamp (UTC).
    hour_decimal is a float like 13.5 for 13:30.
    """
    ts = ensure_utc(timestamp)
    hour = ts.hour + ts.minute / 60.0 + ts.second / 3600.0 + ts.microsecond / 3_600_000_000.0
    day_of_year = ts.timetuple().tm_yday
    weekday = ts.weekday()  # 0 = Monday
    return hour, day_of_year, weekday
