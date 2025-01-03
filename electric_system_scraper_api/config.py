import os
import logging

# Get the current directory where this script is located
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Create a log directory under the current project directory if it doesn't exist
LOG_DIR = os.path.join(BASE_DIR, "logs")

# Check if the logs directory exists, if not create it
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

LOG_FILE = os.path.join(LOG_DIR, 'application.log')

# Set up the logger explicitly
logger = logging.getLogger("appLogger")  # Named logger to avoid conflicts
logger.setLevel(logging.DEBUG)  # Capture all log levels (DEBUG, INFO, etc.)

# Create a file handler that logs to the log file
file_handler = logging.FileHandler(LOG_FILE, mode='a')
file_handler.setLevel(logging.DEBUG)  # Log to file in append mode

# Create a console handler that logs to the console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)  # Log to console

# Define log format
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Test the logger works by writing different levels of logs
logger.debug("Debug level message")
logger.info("Info level message")
logger.warning("Warning level message")
logger.error("Error level message")
logger.critical("Critical level message")

# Check if the log file path is correct
print(f"Log file is being written to: {os.path.abspath(LOG_FILE)}")

# Configuration settings dictionary
CONFIG = {
    # URL to fetch data from (could be an external scraper or API endpoint)
    "SCRAPER_URL": "http://ets.aeso.ca/ets_web/ip/Market/Reports/ActualForecastWMRQHReportServlet",
    
    # Correct database URL with SQLite configuration
    "DATABASE_URL": f"sqlite:///{os.path.join(BASE_DIR, 'db_storage', 'actual_forecast_report.db')}",
    
    # Redis server configuration for caching (can be used for performance enhancement)
    "REDIS_HOST": "localhost",  # Host for the Redis server
    "REDIS_PORT": 6379,         # Port on which Redis is running
    "REDIS_DB": 0,              # Database index in Redis (default is 0)
}
