import redis
from config import CONFIG, logger

class Cache:
    """
    Cache class for interacting with Redis, providing methods to store and retrieve data.
    """

    def __init__(self):
        """
        Initializes the Redis client and establishes a connection using configurations from the settings.
        """
        logger.info("Initializing Redis cache.")
        try:
            self.client = redis.StrictRedis(
                host=CONFIG["REDIS_HOST"], 
                port=CONFIG["REDIS_PORT"], 
                db=CONFIG["REDIS_DB"]
            )
            logger.info("Redis cache initialized successfully.")
        except Exception as e:
            logger.error(f"Error initializing Redis cache: {str(e)}")
            raise

    def set(self, key, value, ttl=3600):
        """
        Stores a value in Redis under the given key, with an optional Time-to-Live (TTL).

        Args:
            key (str): The key under which to store the value.
            value (str or any serializable type): The value to store in cache.
            ttl (int, optional): Time-to-Live (in seconds). Default is 3600 seconds (1 hour).
        """
        try:
            self.client.set(key, value, ex=ttl)
            logger.debug(f"Set cache for key: {key} with TTL: {ttl} seconds.")
        except Exception as e:
            logger.error(f"Error setting cache for key {key}: {str(e)}")

    def get(self, key):
        """
        Retrieves a value from Redis by its key.

        Args:
            key (str): The key for which to fetch the stored value.

        Returns:
            str or None: The value stored in the cache, or None if not found or error occurs.
        """
        try:
            value = self.client.get(key)
            logger.debug(f"Retrieved cache for key: {key}.")
            return value
        except Exception as e:
            logger.error(f"Error retrieving cache for key {key}: {str(e)}")
            return None
