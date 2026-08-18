import os
import logging
import requests
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

if __name__ == "__main__":
    fetch_data()