import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

CITIES = {
    "paris": (48.8566, 2.3522),
    "london": (51.5074, -0.1278),
    "berlin": (52.5200, 13.4050),
    "new_york": (40.7128, -74.0060),
}

_session = None

def _get_session() -> requests.Session:
    """Resilient session with retry and backoff."""
    global _session
    if _session is None:
        retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 503])
        _session = requests.Session()
        _session.mount("https://", HTTPAdapter(max_retries=retry))
    return _session

def get_weather(city: str = "paris") -> dict:
    city = city.lower().strip()
    if city not in CITIES:
        raise ValueError(f"Unknown city '{city}'. Valid: {list(CITIES.keys())}")

    lat, lon = CITIES[city]
    session = _get_session()

    # 8 second timeout prevents locking Flask worker threads
    resp = session.get(
        OPEN_METEO_URL,
        params={
            "latitude": lat, "longitude": lon,
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code"
        },
        timeout=8 
    )
    resp.raise_for_status()
    data = resp.json()
    current = data.get("current", {})

    return {
        "city": city, "latitude": lat, "longitude": lon,
        "temperature_c": current.get("temperature_2m"),
        "humidity_pct": current.get("relative_humidity_2m"),
        "wind_speed_kmh": current.get("wind_speed_10m"),
        "weather_code": current.get("weather_code"),
        "fetched_at": current.get("time"),
    }