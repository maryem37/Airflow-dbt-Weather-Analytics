import psycopg2
from api_request import mock_fetch_data, fetch_data


def connect_to_db():
    print("Connecting to the PostgreSQL database...")
    try:
        return psycopg2.connect(
            host="db",
            port=5432,
            dbname="db",
            user="db_user",
            password="db_password"
        )
    except psycopg2.Error as e:
        print(f"Database connection failed: {e}")
        raise


def create_table(conn):
    print("Creating table if not exists...")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS dev;
            CREATE TABLE IF NOT EXISTS dev.raw_weather_data (
                id SERIAL PRIMARY KEY,
                city TEXT,
                temperature FLOAT,
                weather_descriptions TEXT,
                wind_speed FLOAT,
                time TIMESTAMP,
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                utc_offset TEXT
            );
        """)
        conn.commit()
        cursor.close()
    except psycopg2.Error as e:
        print(f"Failed to create table: {e}")
        raise


def insert_data(conn, data):
    if not data:
        print("No valid data to insert.")
        return

    print("Inserting data into database...")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO dev.raw_weather_data (city, temperature, weather_descriptions, wind_speed, time, utc_offset)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, (
            data.get('city', 'Unknown'),
            data.get('temperature', 0.0),
            data.get('weather_descriptions', ''),
            data.get('wind_speed', 0.0),
            data.get('time'),
            data.get('utc_offset')
        ))
        conn.commit()
        cursor.close()
    except psycopg2.Error as e:
        print(f"Failed to insert data: {e}")
        raise


def main():
    conn = connect_to_db()
    create_table(conn)
    data = fetch_data()
    if data:
        insert_data(conn, data)
    conn.close()


if __name__ == "__main__":
    main()