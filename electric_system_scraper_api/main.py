from app.api import app  # Import the FastAPI app from the 'api' module inside the 'app' directory
from config import logger  # Import the logger configuration from config.py
import uvicorn  # Uvicorn is an ASGI server used to run FastAPI applications

if __name__ == "__main__":
    # Ensure the logger setup is applied before starting the app
    logger.info("Starting the application...")  # This should now be printed to both console and file

    try:
        # Start the FastAPI application with Uvicorn, which serves the app at host '127.0.0.1' on port 8000
        # Disable Uvicorn's internal logger and let your custom logging handle all logging
        uvicorn.run(app, host="0.0.0.0", port=8000, log_level="debug")
        
        # Log the successful start of the application
        logger.info("Application started successfully.")
    except Exception as e:
        # Log any error that occurs during the application startup
        logger.error(f"Error starting the application: {e}")
