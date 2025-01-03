from fastapi import Depends, HTTPException
from sqlalchemy.orm import Session
from app.database.connection import DatabaseConnection
from config import logger

def get_db_session():
    """
    Dependency to retrieve a new database session for each request.

    Returns:
        Session: SQLAlchemy session object.
    """
    try:
        db = DatabaseConnection().get_session()
        yield db
    finally:
        db.close()
        logger.info("Database session closed.")