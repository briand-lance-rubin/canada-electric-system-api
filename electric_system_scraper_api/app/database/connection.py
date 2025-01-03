import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config import CONFIG, logger

class DatabaseConnection:
    def __init__(self):
        """
        Initializes the database connection using SQLAlchemy.
        """
        try:
            self.engine = create_engine(CONFIG["DATABASE_URL"])  # Create the SQLAlchemy engine
            self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
            logger.info("Database connection established successfully.")
        except Exception as e:
            logger.error(f"Error initializing the database: {e}")
            raise

    def get_session(self):
        """
        Returns a new database session.
        
        Returns:
            Session: SQLAlchemy session object for interacting with the database.
        """
        try:
            return self.SessionLocal()
        except Exception as e:
            logger.error(f"Error creating database session: {e}")
            raise
