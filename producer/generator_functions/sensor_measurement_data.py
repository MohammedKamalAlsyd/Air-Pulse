# producer/main.py
"""
Producer helpers for simulating environmental sensor data.

Each simulate_* function:
 - accepts a `timestamp` (datetime) and optional context (location, etc.)
 - returns a numeric value suitable for storage / regression (float)
 - includes deterministic patterns (diurnal, seasonal, traffic peaks) + randomness

generate_sensor_date(sensor_id, location, timestamp=None)
 - builds a JSON-serializable payload (strings, floats) ready for producing to Kafka.
"""

from datetime import datetime, timezone
import math
import random
import uuid
from typing import Tuple, Optional, Dict, Any

# ---------- Utility helpers ----------

def _time_features(ts: datetime):
    """Return hour_of_day (0-24 float), day_of_year (1-366), and seconds_since_midnight."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    hour = ts.hour + ts.minute / 60.0 + ts.second / 3600.0
    day_of_year = ts.timetuple().tm_yday
    seconds_since_midnight = ts.hour * 3600 + ts.minute * 60 + ts.second
    return hour, day_of_year, seconds_since_midnight

def _clamp(v: float, low: float, high: float) -> float:
    return max(low, min(high, v))

# ---------- Simulation functions (each takes timestamp) ----------

def simulate_pm25(timestamp: datetime, location: Optional[Tuple[float, float]] = None) -> float:
    """
    Simulate PM2.5 (µg/m³)
    - Higher during morning/evening traffic peaks.
    - Random short spikes occasionally.
    """
    hour, day_of_year, _ = _time_features(timestamp)

    # baseline varies with season (slightly higher in winter)
    seasonal = 12 + 6 * math.cos(2 * math.pi * (day_of_year / 365.0))  # winter bump

    # diurnal traffic peaks: morning ~8, evening ~18
    morning_peak = math.exp(-0.5 * ((hour - 8) / 1.8) ** 2)
    evening_peak = math.exp(-0.5 * ((hour - 18) / 2.0) ** 2)
    diurnal = 25 * (0.6 * morning_peak + 0.7 * evening_peak)

    # background random variability and occasional spike
    noise = random.gauss(0, 3)
    spike = 0.0
    if random.random() < 0.02:  # 2% chance of a short spike (construction, wildfire smoke, etc.)
        spike = random.uniform(20, 80)

    value = seasonal + diurnal + noise + spike
    return round(_clamp(value, 0.0, 500.0), 2)

def simulate_co2(timestamp: datetime, location: Optional[Tuple[float, float]] = None) -> float:
    """
    Simulate CO2 (ppm)
    - Baseline around ~400-420 (ambient)
    - Indoor/urban influence raises it during occupied times (daytime)
    """
    hour, day_of_year, _ = _time_features(timestamp)

    # ambient baseline (slightly seasonal)
    seasonal = 410 + 6 * math.sin(2 * math.pi * (day_of_year / 365.0))

    # human activity scale (higher during day)
    activity = 0.0
    if 7 <= hour <= 20:
        # occupancy/work hours
        activity = 150 * math.exp(-0.5 * ((hour - 13) / 4.0) ** 2)

    noise = random.gauss(0, 10)

    # occasional local accumulation spike
    spike = random.uniform(0, 0)
    if random.random() < 0.01:
        spike = random.uniform(50, 300)

    value = seasonal + activity + noise + spike
    return round(_clamp(value, 300.0, 5000.0), 2)

def simulate_no2(timestamp: datetime, location: Optional[Tuple[float, float]] = None) -> float:
    """
    Simulate NO2 (ppb or µg/m3 depending on unit, treat as numeric index)
    - Traffic-correlated: morning & evening peaks.
    """
    hour, day_of_year, _ = _time_features(timestamp)

    diurnal = 0.0
    diurnal += 30 * math.exp(-0.5 * ((hour - 8) / 1.5) ** 2)   # morning
    diurnal += 28 * math.exp(-0.5 * ((hour - 18) / 1.8) ** 2)  # evening

    background = 8 + 3 * math.cos(2 * math.pi * (day_of_year / 365.0))
    noise = random.gauss(0, 2)

    # occasional short high-emission events
    event = 0.0
    if random.random() < 0.015:
        event = random.uniform(20, 80)

    value = background + diurnal + noise + event
    return round(_clamp(value, 0.0, 1000.0), 2)

def simulate_temperature(timestamp: datetime, location: Optional[Tuple[float, float]] = None) -> float:
    """
    Simulate temperature (°C)
    - Uses seasonal (day_of_year) and diurnal (hour) sinusoids + noise.
    - Optionally modulate by latitude roughly if location given (simple approximation).
    """
    hour, day_of_year, _ = _time_features(timestamp)

    # latitude-based seasonal amplitude modifier (optional)
    lat = location[0] if location else 0.0
    lat_factor = _clamp(abs(lat) / 90.0, 0.0, 1.0)  # higher lat -> larger seasonality
    # baseline average
    annual_avg = 15.0 - 10.0 * lat_factor  # crude: higher lat -> colder on avg

    # seasonal variation
    seasonal_amp = 12.0 * (0.5 + 0.5 * lat_factor)
    seasonal = seasonal_amp * math.sin(2 * math.pi * (day_of_year / 365.0 - 0.25))

    # diurnal variation
    diurnal_amp = 7.0
    diurnal = diurnal_amp * math.sin(2 * math.pi * (hour / 24.0 - 0.25))

    noise = random.gauss(0, 0.8)

    value = annual_avg + seasonal + diurnal + noise
    return round(_clamp(value, -50.0, 60.0), 2)

def simulate_humidity(timestamp: datetime, location: Optional[Tuple[float, float]] = None, temperature_c: Optional[float] = None) -> float:
    """
    Simulate relative humidity (%)
    - Rough inverse relationship with temperature.
    """
    if temperature_c is None:
        temperature_c = simulate_temperature(timestamp, location)

    hour, day_of_year, _ = _time_features(timestamp)

    # base humidity roughly decreases with temperature
    base = 65 - (temperature_c - 10) * 1.2
    # diurnal and noisy variation
    diurnal = 8 * math.cos(2 * math.pi * (hour / 24.0))
    noise = random.gauss(0, 5)

    value = base + diurnal + noise
    return round(_clamp(value, 0.0, 100.0), 2)

def simulate_noise_level(timestamp: datetime, location: Optional[Tuple[float, float]] = None) -> float:
    """
    Simulate ambient noise level (dB)
    - Higher during day, lower at night, with random short spikes.
    """
    hour, day_of_year, _ = _time_features(timestamp)

    # day baseline and night dip
    day_component = 50 + 20 * math.exp(-0.5 * ((hour - 14) / 6.0) ** 2)
    # traffic peaks bump
    traffic_bumps = 10 * math.exp(-0.5 * ((hour - 8) / 1.5) ** 2) + 8 * math.exp(-0.5 * ((hour - 18) / 1.8) ** 2)

    noise = random.gauss(0, 3)

    spike = 0.0
    if random.random() < 0.03:
        spike = random.uniform(10, 25)  # passing siren, heavy truck, construction

    value = day_component + traffic_bumps + noise + spike
    return round(_clamp(value, 20.0, 140.0), 2)

def simulate_uv_index(timestamp: datetime, location: Optional[Tuple[float, float]] = None) -> float:
    """
    Simulate UV index (0-11+)
    - Zero at night; smooth bell curve around solar noon; scaled by season and latitude.
    """
    hour, day_of_year, _ = _time_features(timestamp)

    # approximate solar noon effect: bell around 12-13
    if hour < 6 or hour > 18:
        return 0.0

    # rough day length effect: longer days in summer -> higher UV
    seasonal_scale = 1.0 + 0.5 * math.sin(2 * math.pi * (day_of_year / 365.0 - 0.25))
    lat = location[0] if location else 0.0
    lat_factor = _clamp(1.0 - abs(lat) / 60.0, 0.2, 1.0)  # lower at high latitudes

    bell = math.exp(-0.5 * ((hour - 12.5) / 2.5) ** 2)
    max_uv = 9.0 * seasonal_scale * lat_factor
    noise = random.gauss(0, 0.2)

    value = max_uv * bell + noise
    return round(_clamp(value, 0.0, 15.0), 2)

# ---------- Top-level generator ----------

def generate_sensor_date(sensor_id: str, location: Tuple[float, float], timestamp: Optional[datetime] = None) -> Dict[str, Any]:
    """
    Generate a full sensor payload.
    - sensor_id: unique id string for the sensor (not UUID)
    - location: (latitude, longitude)
    - timestamp: datetime; if None, uses current UTC time

    Returns a JSON-serializable dict (uuid and timestamp are strings).
    """
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)
    # Ensure timestamp is timezone-aware
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)

    # Generate components
    pm25 = simulate_pm25(timestamp, location)
    co2 = simulate_co2(timestamp, location)
    no2 = simulate_no2(timestamp, location)
    temperature = simulate_temperature(timestamp, location)
    humidity = simulate_humidity(timestamp, location, temperature)
    noise_levels = simulate_noise_level(timestamp, location)
    uv_index = simulate_uv_index(timestamp, location)

    payload = {
        "id": str(uuid.uuid4()),
        "sensor_id": sensor_id,
        "location": (float(location[0]), float(location[1])),
        "timestamp": timestamp.isoformat(),
        "pm25": pm25,
        "co2": co2,
        "no2": no2,
        "temperature": temperature,
        "humidity": humidity,
        "noise_levels": noise_levels,
        "uv_index": uv_index
    }

    return payload