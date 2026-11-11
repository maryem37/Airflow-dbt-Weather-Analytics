import psycopg2
from api_request import mock_fetch_data, fetch_data

def connect_to_db():
    print("Connecting to the PostgreSQL database...")
    try:
        conn = psycopg2.connect(
            host="db",
            port=5432,
            dbname="db",
            user="db_user",
            password="db_password"
        )
        return conn
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
                wind_speed FLOAT
            );
        """)
        conn.commit()  # <-- Commits schema & table creation to PostgreSQL
        cursor.close()
    except psycopg2.Error as e:
        print(f"Failed to create table: {e}")
        raise

def insert_data(conn, data):
    print("Inserting data into database...")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO dev.raw_weather_data (city, temperature, weather_descriptions, wind_speed)
            VALUES (%s, %s, %s, %s);
        """, (data['city'], data['temperature'], data['weather_descriptions'], data['wind_speed']))
        conn.commit()  # <-- Commits inserted records
        cursor.close()
    except psycopg2.Error as e:
        print(f"Failed to insert data: {e}")
        raise

def main():
    conn = connect_to_db()
    create_table(conn)
    data = fetch_data()  # or mock_fetch_data()
    insert_data(conn, data)
    conn.close()

if __name__ == "__main__":
    main()