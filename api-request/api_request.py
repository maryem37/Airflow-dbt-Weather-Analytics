import requests

API_KEY = "f9831b22375ea1822da66eaa55282fea"
API_URL = "http://api.weatherstack.com/current"


def fetch_data(city="New York"):
    print(f"Fetching weather data from Weatherstack API for '{city}'...")
    params = {
        "access_key": API_KEY,
        "query": city
    }

    try:
        response = requests.get(API_URL, params=params)
        response.raise_for_status()
        raw_data = response.json()

        if "error" in raw_data:
            print(f"API Error ({raw_data['error'].get('code')}): {raw_data['error'].get('info', 'Unknown error')}")
            return None

        print("API response received successfully.")

        location = raw_data.get("location", {})
        current = raw_data.get("current", {})

        descriptions = current.get("weather_descriptions", [])
        desc_str = ", ".join(descriptions) if isinstance(descriptions, list) else str(descriptions)

        return {
            "city": location.get("name", city),
            "temperature": float(current.get("temperature", 0.0)),
            "weather_descriptions": desc_str,
            "wind_speed": float(current.get("wind_speed", 0.0)),
            "time": location.get("localtime"),
            "utc_offset": location.get("utc_offset")
        }

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        raise


def mock_fetch_data():
    print("Using mock data (offline mode)...")
    return {
        "city": "New York",
        "temperature": 16.0,
        "weather_descriptions": "Partly Cloudy",
        "wind_speed": 10.0,
        "time": "2025-05-07 00:17:00",
        "utc_offset": "-4.0"
    }