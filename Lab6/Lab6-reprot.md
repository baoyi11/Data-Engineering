# Lab 6 Report: 

# APIs & Web Services II



## Step 0 - Install Dependencies

To install the necessary Python libraries required for adding security (JWT and API keys), rate limiting, OpenAPI (Swagger) documentation, and making external HTTP requests.  

```powershell
pip install flask-jwt-extended flask-limiter flask-smorest marshmallow requests
```

![image-20260518091516779](Lab6-reprot.assets/image-20260518091516779.png)

![image-20260518091537653](Lab6-reprot.assets/image-20260518091537653.png)

## Step 1 - API Key Authentication

### 1a: Define the Key Store and Decorator

Implement a static API key store and create Python decorators to enforce authentication (verifying identity) and authorization (verifying roles/permissions).  

```python
from functools import wraps
from flask import request, jsonify, g
import secrets

# API Key store mapping keys to client identities and roles
API_KEYS = {
    "dev-key-sensor-read-only": {"client": "dashboard", "role": "reader"},
    "dev-key-sensor-write": {"client": "pipeline", "role": "writer"},
    "dev-key-admin": {"client": "admin", "role": "admin"},
}

def generate_api_key() -> str:
    """Generate a cryptographically secure 32-byte API key."""
    return secrets.token_urlsafe(32)

def require_api_key(f):
    """Decorator: validates X-API-Key header."""
    @wraps(f)
    def decorated(*args, **kwargs):
        key = request.headers.get("X-API-Key")
        if not key:
            return jsonify({"status": "error", "code": 401, "message": "X-API-Key header is required."}), 401
        client = API_KEYS.get(key)
        if not client:
            return jsonify({"status": "error", "code": 401, "message": "Invalid API key."}), 401

        g.api_client = client # Store client info in Flask g context
        return f(*args, **kwargs)
    return decorated

def require_role(role: str):
    """Decorator factory: checks that g.api_client has the given role."""
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            client = g.get("api_client", {})
            allowed = {
                "reader": ["reader", "writer", "admin"],
                "writer": ["writer", "admin"],
                "admin": ["admin"],
            }
            if client.get("role") not in allowed.get(role, []):
                return jsonify({"status": "error", "code": 403, "message": f"Role {role} or higher required."}), 403
            return f(*args, **kwargs)
        return decorated
    return decorator
```

![image-20260518091908968](Lab6-reprot.assets/image-20260518091908968.png)

### 1b: Apply to Endpoints

Protect the existing Lab 5 endpoints so that only authenticated and authorized users can access or write data.  

```python
# sensor_api/app.py (Additions)
from auth import require_api_key, require_role # Import the new decorators

# Protect the GET sensors list endpoint
@blp.route("/sensors")
@require_api_key # Read access requires just a valid key
def list_sensors():
    # ... (existing Lab 5 code) ...

# Protect the POST readings endpoint
@blp.route("/readings", methods=["POST"])
@require_api_key
@require_role("writer") # Writing requires 'writer' or 'admin' role
def create_reading():
    # ... (existing Lab 5 code) ...
```

![image-20260518092401296](Lab6-reprot.assets/image-20260518092401296.png)

![image-20260518104637153](Lab6-reprot.assets/image-20260518104637153.png)

![image-20260518104710351](Lab6-reprot.assets/image-20260518104710351.png)

## Step 2 - JWT Authentication

### 2a: Configure Flask-JWT-Extended

Set up the JWT Manager within the Flask application to handle token signing and expiration.  

```python
# sensor_api/app.py
from flask_jwt_extended import JWTManager, create_access_token, jwt_required, get_jwt_identity, get_jwt
from datetime import timedelta

app.config["JWT_SECRET_KEY"] = "CHANGE-ME-IN-PRODUCTION" # Secret for HMAC
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(hours=1) # Tokens expire in 1 hour

jwt = JWTManager(app) # Initialize JWT
```

![image-20260518092824170](Lab6-reprot.assets/image-20260518092824170.png)

### 2b: Login Endpoint (Issues JWT)

Provide an endpoint for users to exchange a username and password for an access token.  

```python
# Simulated user store
USERS = {
    "alice": {"password": "secret123", "role": "admin"},
    "bob": {"password": "pass456", "role": "reader"},
}

@blp.route("/auth/login", methods=["POST"])
@limiter.limit("10 per minute; 3 per second") 
def login():
    body = request.get_json(silent=True)
    if not body:
        return jsonify({"status": "error", "message": "JSON body required."}), 400

    username = body.get("username", "")
    password = body.get("password", "")
    user = USERS.get(username)

    if not user or not hmac.compare_digest(user["password"], password):
        return jsonify({"status": "error", "message": "Invalid credentials."}), 401

    token = create_access_token(identity=username, additional_claims={"role": user["role"]})
    return jsonify({
        "status": "success",
        "access_token": token,
        "token_type": "Bearer",
        "expires_in": 3600
    }), 200

```

![image-20260518104506105](Lab6-reprot.assets/image-20260518104506105.png)

![image-20260518104451686](Lab6-reprot.assets/image-20260518104451686.png)

### 2c: JWT-Protected Endpoint

Create an endpoint to demonstrate verifying and decoding a JWT Bearer token.  

```python
@blp.route("/me")
@jwt_required() 
def me():
    identity = get_jwt_identity() 
    claims = get_jwt() 
    return jsonify({
        "status": "success",
        "data": {
            "username": identity,
            "role": claims.get("role"),
        },
    }), 200
```

![image-20260518104321044](Lab6-reprot.assets/image-20260518104321044.png)

## Step 3 - Rate Limiting

 Protect the API from being overwhelmed (DDoS or run-away scripts) by limiting the frequency of requests.  

```python
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

limiter = Limiter(
    app=app,
    key_func=get_remote_address, # Rate limit per client IP
    default_limits=["500 per day", "100 per hour"],
    storage_uri="memory://",
    headers_enabled=True, # Adds X-RateLimit headers
)

@limiter.exempt
@limiter.limit("10 per minute; 3 per second") 
@limiter.limit("300 per minute")

```

![image-20260518100519600](C:\Users\xback20040219\AppData\Roaming\Typora\typora-user-images\image-20260518100519600.png)

![image-20260518104209949](Lab6-reprot.assets/image-20260518104209949.png)

![image-20260518104227567](Lab6-reprot.assets/image-20260518104227567.png)

![image-20260518104252313](Lab6-reprot.assets/image-20260518104252313.png)

## Step 4 - Swagger UI

Generate an interactive API specification interface using OpenAPI 3.0.  

```python
# Configure OpenAPI
app.config.update({
    "API_TITLE": "Sensor Data API",
    "API_VERSION": "v1",
    "OPENAPI_VERSION": "3.0.3",
    "OPENAPI_URL_PREFIX": "/",
    "OPENAPI_SWAGGER_UI_PATH": "/swagger-ui",
    "OPENAPI_SWAGGER_UI_URL": "https://cdn.jsdelivr.net/npm/swagger-ui-dist/",
    "OPENAPI_REDOC_PATH": "/redoc",
})

from flask_smorest import Api
api = Api(app)
```

![image-20260518093324411](Lab6-reprot.assets/image-20260518093324411.png)

## Step 5 - Weather Enrichment Endpoint

### 5a: Weather Fetcher Helper

Build a resilient HTTP client to fetch external data (Open-Meteo API) using retry logic and timeouts.  

```python
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
        " weather_code ": current . get (" weather_code ") ,
        " fetched_at ": current . get (" time ") ,
    }
```

![image-20260518101019143](Lab6-reprot.assets/image-20260518101019143.png)

### 5b: Enrichment Endpoint

Combine local Kafka sensor readings with the external weather data to create a "data enrichment" response.  

```python
from weather_utils import get_weather, CITIES

@blp.route("/enriched/<sensor_type>")
@require_api_key
def enriched_reading(sensor_type: str):
    city = request.args.get("city", "paris").lower()
    if city not in CITIES:
        return jsonify({"status": "error", "message": f"Unknown city. Valid: {list(CITIES.keys())}"}), 400

    try:
        readings = get_latest_readings(sensor_type, n=1)
        sensor_data = readings[0] if readings else None
    except Exception as exc:
        app.logger.error("Kafka error: %s", exc)
        return jsonify({"status": "error", "message": "Failed to fetch sensor reading."}), 503

    try:
        weather = get_weather(city)
    except Exception as exc:
        app.logger.error("Weather API error: %s", exc)
        return jsonify({"status": "success", "sensor": sensor_data, "weather": None, "warning": "Weather service unavailable."}), 200

    diff = None
    if sensor_type == "temperature" and sensor_data and weather:
        diff = round((sensor_data.get("value", 0) - weather.get("temperature_c", 0)), 2)

    return jsonify({
        "status": "success",
        "sensor": sensor_data,
        "weather": weather,
        "context": {"outdoor_vs_sensor_temp": diff}
    }), 200
```

![image-20260518104049424](Lab6-reprot.assets/image-20260518104049424.png)

## Step 6 - End-to-End Test

1. Start the API:

```powershell
python sensor_api/app.py
```

![image-20260518100945262](Lab6-reprot.assets/image-20260518100945262.png)

2. Test without auth (Expect 401):  

```powershell
curl.exe -s http://localhost:5000/api/v1/sensors | python -m json.tool
```

![image-20260518101301557](Lab6-reprot.assets/image-20260518101301557.png)

3. Test with valid API key:  

```powershell
curl.exe -s http://localhost:5000/api/v1/sensors -H "X-API-Key: dev-key-sensor-read-only" | python -m json.tool
```

![image-20260518101233514](Lab6-reprot.assets/image-20260518101233514.png)

4. POST with reader key (Expect 403 Forbidden):  

```powershell
curl.exe -s -X POST http://localhost:5000/api/v1/readings -H "X-API-Key: dev-key-sensor-read-only" -H "Content-Type: application/json" -d '{"sensor": "temperature", "value": 29.5}' | python -m json.tool
```

![image-20260518101341268](Lab6-reprot.assets/image-20260518101341268.png)

5. JWT Login:  

```powershell
curl.exe -s -X POST http://localhost:5000/api/v1/auth/login -H "Content-Type: application/json" -d '{\"username\":\"alice\", \"password\":\"secret123\"}' | python -m json.tool
```

![image-20260518101742774](Lab6-reprot.assets/image-20260518101742774.png)

6. Use JWT to call `/me`:  

```powershell
curl.exe -s http://localhost:5000/api/v1/me -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJmcmVzaCI6ZmFsc2UsImlhdCI6MTc3OTA3MDY0MywianRpIjoiMzAyZTdjOWQtY2I4ZC00NDg1LTg1MzgtOGQwMWExYmI2MzdlIiwidHlwZSI6ImFjY2VzcyIsInN1YiI6ImFsaWNlIiwibmJmIjoxNzc5MDcwNjQzLCJjc3JmIjoiYWExOTVlMjAtNmY3My00YjllLTgwMTktNDExNjI2YTg5NGVjIiwiZXhwIjoxNzc5MDc0MjQzLCJyb2xlIjoiYWRtaW4ifQ.5VgODlEk9OtwyFf9OrVfBAj-a5BI06IYfqtJYjPBs1w" | python -m json.tool
```

![image-20260518102122306](Lab6-reprot.assets/image-20260518102122306.png)

7. Test Rate Limiting (Run script 15 times):  

```powershell
for ($i=1; $i -le 15; $i++) {
    $response = curl.exe -s -X POST http://localhost:5000/api/v1/auth/login -H "Content-Type: application/json" -d '{\"username\":\"x\", \"password\":\"x\"}' | ConvertFrom-Json
    if ($response.message -match "Rate limit") {
        Write-Host "[$i] status=$($response.status) (Rate limit triggered: $($response.message))" -ForegroundColor Red
    } else {
        Write-Host "[$i] status=$($response.status) (Normally rejected: $($response.message))"
    }
}
# Result: First 10 requests return 401 (invalid creds), 11th request returns:
# { "message": "10 per 1 minute", "status": "error" } (Status 429)
```

![image-20260518102519558](Lab6-reprot.assets/image-20260518102519558.png)

8. Enriched Endpoint:  

```powershell
curl.exe -s "http://localhost:5000/api/v1/enriched/temperature?city=paris" -H "X-API-Key: dev-key-sensor-read-only" | python -m json.tool
```

![image-20260518102914354](Lab6-reprot.assets/image-20260518102914354.png)

9. Swagger UI

```powershell
echo " Open : http :// localhost :5000/ swagger -ui"
```

![image-20260518103627945](Lab6-reprot.assets/image-20260518103627945.png)