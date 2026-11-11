import psycopg2
from api_request import mock_fetch_data  # ou fetch_data si vous utilisez l'API réelle


def connect_to_db():
    """Établit la connexion à la base de données PostgreSQL Docker."""
    print("Connecting to the PostgreSQL database...")
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5000,
            dbname="db",
            user="db_user",
            password="db_password"
        )
        print("Database connection established.")
        return conn
    except psycopg2.Error as e:
        print(f"Database connection failed: {e}")
        raise


def create_table(conn):
    """Crée le schéma 'dev' et la table 'dev.raw_weather_data' s'ils n'existent pas."""
    print("Creating table if not exist...")
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE SCHEMA IF NOT EXISTS dev;

                CREATE TABLE IF NOT EXISTS dev.raw_weather_data (
                    id SERIAL PRIMARY KEY,
                    city TEXT,
                    temperature FLOAT,
                    weather_descriptions TEXT,
                    wind_speed FLOAT,
                    time TIMESTAMP,
                    inserted_at TIMESTAMP DEFAULT NOW(),
                    utc_offset TEXT
                );
            """)
        conn.commit()
        print("Table was created.")
    except psycopg2.Error as e:
        print(f"Failed to create table: {e}")
        raise


def insert_weather_record(conn, data):
    """Extrait les champs de la réponse API et insère une ligne dans dev.raw_weather_data."""
    if not data:
        print("No data to insert.")
        return

    print("Inserting record into dev.raw_weather_data...")
    try:
        location = data.get("location", {})
        current = data.get("current", {})

        city = location.get("name")
        temperature = current.get("temperature")
        descriptions = current.get("weather_descriptions", [])
        weather_descriptions = ", ".join(descriptions) if isinstance(descriptions, list) else str(descriptions)
        wind_speed = current.get("wind_speed")
        time_str = location.get("localtime")
        utc_offset = location.get("utc_offset")

        insert_query = """
            INSERT INTO dev.raw_weather_data (city, temperature, weather_descriptions, wind_speed, time, utc_offset)
            VALUES (%s, %s, %s, %s, %s, %s);
        """

        with conn.cursor() as cursor:
            cursor.execute(insert_query, (city, temperature, weather_descriptions, wind_speed, time_str, utc_offset))
        conn.commit()
        print(f"Record successfully inserted for city: {city}")
    except psycopg2.Error as e:
        print(f"Failed to insert record: {e}")
        raise


if __name__ == "__main__":
    conn = None
    try:
        # 1. Connexion à PostgreSQL
        conn = connect_to_db()

        # 2. Création de la table
        create_table(conn)

        # 3. Récupération des données météo
        weather_data = mock_fetch_data()

        # 4. Insertion dans la base de données
        insert_weather_record(conn, weather_data)

    finally:
        if conn:
            conn.close()
            print("Database connection closed.")