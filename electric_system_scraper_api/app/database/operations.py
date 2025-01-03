from sqlalchemy.exc import SQLAlchemyError
from app.database.connection import DatabaseConnection  # Corrected import path
from app.database.models import HistoricalDemand
from config import logger
import pandas as pd

class DatabaseOperations:
    def __init__(self):
        # Establish a session with the database
        self.db = DatabaseConnection().get_session()

    def save_data(self, data):
        """
        Save multiple records to the database using bulk inserts, avoiding duplicates based on 'Date'.
        """
        try:
            # Prepare data for bulk insert
            records = [
                {
                    "date": row["Date (HE)"],  # Convert 'Date (HE)' column to database-compatible datetime
                    "actual_demand": row.get("Actual Posted Pool Price"),  # Map columns to database fields
                    "forecast_demand": row.get("Forecast Pool Price"),
                    "forecast_ail": row.get("Forecast AIL"),
                    "actual_ail": row.get("Actual AIL"),
                    "forecast_ail_actual_difference": row.get("Forecast AIL & Actual AIL Difference"),
                }
                for row in data
            ]
            
            # Fetch existing dates in the database
            existing_dates = set(
                [record.date for record in self.db.query(HistoricalDemand.date).all()]
            )

            # Filter out the records with a date that already exists in the database
            records_to_insert = [record for record in records if record["date"] not in existing_dates]
            logger.info(f"Found {len(records_to_insert)} new records to insert.")

            if records_to_insert:
                # Use bulk_insert_mappings for efficient insertion
                self.db.bulk_insert_mappings(HistoricalDemand, records_to_insert)
                self.db.commit()  # Commit the transaction
                logger.info(f"Successfully inserted {len(records_to_insert)} records.")
            else:
                logger.info("No new records to insert. All dates already exist in the database.")

        except SQLAlchemyError as e:
            # Rollback the transaction in case of errors
            self.db.rollback()
            logger.error(f"Error during bulk insert: {e}")
            raise
        finally:
            self.db.close()  # Ensure the database session is closed

    def fetch_all_data(self):
        """
        Fetch all data from the database as a Pandas DataFrame.
        """
        try:
            logger.info("Fetching all data from the database...")
            # Query all records from the HistoricalDemand table
            result = self.db.query(HistoricalDemand).all()
            data = [
                {
                    "Date": record.date,  # Map database fields to dictionary keys
                    "Actual Demand": record.actual_demand,
                    "Forecast Demand": record.forecast_demand,
                    "Forecast AIL": record.forecast_ail,
                    "Actual AIL": record.actual_ail,
                    "Forecast AIL & Actual AIL Difference": record.forecast_ail_actual_difference,
                }
                for record in result
            ]
            logger.info(f"Fetched {len(data)} records from the database.")
            return pd.DataFrame(data)  # Convert the data to a Pandas DataFrame
        except SQLAlchemyError as e:
            logger.error(f"Error fetching data from the database: {e}")
            raise
