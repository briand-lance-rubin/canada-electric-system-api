import requests
from config import logger  # Correct import path for logger

class Fetcher:
    """
    Fetcher class for retrieving raw HTML content from a given URL.
    """
    def __init__(self, url):
        """
        Initializes the Fetcher with the given URL.

        Args:
            url (str): The URL to fetch data from.
        """
        self.url = url

    def fetch(self):
        """
        Fetches the raw HTML content from the specified URL.

        Returns:
            str: Raw HTML content as a string.

        Raises:
            Exception: If the request fails or the server returns an error response.
        """
        logger.info(f"Fetching data from URL: {self.url}")
        response = requests.get(self.url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        logger.info("Data fetched successfully.")
        return response.text
