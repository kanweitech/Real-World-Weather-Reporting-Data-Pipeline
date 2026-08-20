import os
import logging
import requests
import psycopg2
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def fetch_data():
    load_dotenv() 
    API_KEY = os.getenv("WEATHERSTACK_API_KEY")
    URL = f"http://api.weatherstack.com/current?access_key={API_KEY}&query=New York"
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
    except psycopg2.error as e:
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

if __name__ == "__main__":
    #fetch_data()
    conn = connect_to_db()
    create_schema_table(conn)