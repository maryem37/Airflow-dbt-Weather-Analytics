import requests

# Clé API et URL de base
API_KEY = "f9831b22375ea1822da66eaa55282fea"
API_URL = "http://api.weatherstack.com/current"


def fetch_data(city="New York"):
    """Effectue une vraie requête auprès de l'API Weatherstack."""
    print(f"Fetching weather data from Weatherstack API for '{city}'...")

    params = {
        "access_key": API_KEY,
        "query": city
    }

    try:
        response = requests.get(API_URL, params=params)
        response.raise_for_status()  # Lève une exception si le statut HTTP est en erreur (4xx, 5xx)

        data = response.json()

        # Vérification des erreurs renvoyées dans le corps JSON de l'API
        if "error" in data:
            print(f"API Error ({data['error'].get('code')}): {data['error'].get('info', 'Unknown error')}")
            return None

        print("API response received successfully.")
        return data

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        raise


def mock_fetch_data():
    """Simule une réponse de l'API sans consommer de crédit."""
    print("Using mock data (offline mode)...")
    return {
        'request': {
            'type': 'City',
            'query': 'New York, United States of America',
            'language': 'en',
            'unit': 'm'
        },
        'location': {
            'name': 'New York',
            'country': 'United States of America'
        },
        'current': {
            'temperature': 15,
            'weather_descriptions': ['Partly cloudy'],
            'humidity': 62
        }
    }


if __name__ == "__main__":
    # --- MODE TEST / MOCK ---
    data = mock_fetch_data()
    print(data)

    # --- MODE PRODUCTION (Décommentez la ligne ci-dessous pour utiliser l'API réelle) ---
    # data = fetch_data("New York")
    # if data:
    #     print(data)