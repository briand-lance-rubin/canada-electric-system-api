from sqlalchemy import Column, Integer, Float, DateTime
from sqlalchemy.orm import declarative_base

# Define the base class for SQLAlchemy models
Base = declarative_base()

class HistoricalDemand(Base):
    """
    Represents a historical demand record in the database.
    """
    __tablename__ = "historical_demand"

    # Define the columns for the 'historical_demand' table
    id = Column(Integer, primary_key=True, index=True)  # Primary key
    date = Column(DateTime, index=True)  # Date and time for the data
    actual_demand = Column(Float)  # Actual posted pool price
    forecast_demand = Column(Float)  # Forecast pool price
    forecast_ail = Column(Integer)  # Forecast AIL
    actual_ail = Column(Integer)  # Actual AIL
    forecast_ail_actual_difference = Column(Float)  # Difference between forecasted and actual AIL
