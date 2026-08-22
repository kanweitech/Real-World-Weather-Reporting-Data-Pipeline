import os
import logging
import requests
import psycopg2
from dotenv import load_dotenv
from urllib.parse import quote

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def fetch_data():
    load_dotenv() 
    API_KEY = os.getenv("WEATHERSTACK_API_KEY")
    CITY = quote("New York")
    URL = f"http://api.weatherstack.com/current?access_key={API_KEY}&query={CITY}"
    logger.info("Fetching weather data from weatherstack API...")
    try:
        response = requests.get(URL)
        response.raise_for_status()
        logger.info("API response received successfully.")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"API request error: {e}", exc_info=True)
        raise

def connect_to_db():
    logger.info("connecting to the PostgreSQL database.")
    try:
        conn = psycopg2.connect(
            host = "localhost",
            port = 5000,
            dbname = "dw",
            user = "dw_user",
            password = "dw_password"
        )
        logger.info("Database connection is established.")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Database connection failed: {e}", exc_info=True)
        raise

def create_schema_table(conn):
    logger.info("Creating schema and table if not exist...")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS dev;
            CREATE TABLE IF NOT EXISTS dev.weather_report (
                id SERIAL PRIMARY KEY,
                city TEXT,
                temperature FLOAT,
                weather_description TEXT,
                wind_speed FLOAT,
                time TIMESTAMP,
                inserted_at TIMESTAMP DEFAULT NOW(),
                utc_offset TEXT
            );
        """)
        conn.commit()
        logger.info("Schema and table ensured.")
    except psycopg2.Error as e:
        logger.error(f"Failed to create schema or table: {e}", exc_info=True)
        raise

def insert_records(conn, data):
    logger.info("Inserting weather data into the database...")
    try:
        cursor = conn.cursor()
        weather = data['current']
        location = data['location']
        logger.info(f"Preparing insert for city: {location['name']}")

        cursor.execute("""
            INSERT INTO dev.weather_report (
                city,
                temperature,
                weather_description,
                wind_speed,
                time,
                inserted_at,
                utc_offset
            ) VALUES (%s, %s, %s, %s, %s, NOW(), %s)
        """, (
            location['name'],
            weather['temperature'],
            weather['weather_descriptions'][0],
            weather['wind_speed'],
            location['localtime'],
            location['utc_offset']
        ))
        conn.commit()
        logger.info("Insert successfully completed")
    except psycopg2.Error as e:
        logger.error(f"Error inserting data into the database: {e}", exc_info=True)
        raise

def main():
    try:
        logger.info("Starting weather data ETL process...")
        data = fetch_data()
        conn = connect_to_db()
        create_schema_table(conn)
        insert_records(conn, data)
    except Exception as e:
        logger.error(f"An error occurred during execution: {e}", exc_info=True)
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Database connection closed")


if __name__ == "__main__":
    main()