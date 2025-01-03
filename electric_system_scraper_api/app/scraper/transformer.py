import pandas as pd
from config import logger

class Transformer:
    """
    Transforms raw data into the required format, cleaning and validating it.
    """

    def transform_data(self, raw_data, batch_size=None):
        """
        Transforms the raw data into the required format, cleaning and validating it.
        
        Args:
            raw_data (list of dict): Raw data from scraping process.
            batch_size (int, optional): The batch size for processing the data. If provided, data will be processed in batches.
        
        Returns:
            pd.DataFrame or list: Cleaned and validated DataFrame, or list of batches if batch_size is provided.
        """
        # Convert raw data to DataFrame
        df = pd.DataFrame(raw_data)

        # Log the raw data before cleaning
        logger.info(f"Raw data before cleanup: {df.head()}")

        # Date validation step: Ensure the 'Date (HE)' column is valid
        df['Date (HE)'] = pd.to_datetime(df['Date (HE)'], errors='coerce', format="%m/%d/%Y %H")  # Convert to datetime, invalid dates become NaT
        invalid_dates = df[df['Date (HE)'].isna()]
        logger.warning(f"Rows with invalid dates: {len(invalid_dates)}")
        logger.debug(f"Invalid date rows: {invalid_dates}")

        # Handle invalid rows (optional: handle or drop invalid rows)
        df = df.dropna(subset=['Date (HE)'])  # Remove rows with invalid dates

        # Clean and convert numeric columns to proper data types
        numeric_columns = [
            "Forecast Pool Price", "Actual Posted Pool Price", 
            "Forecast AIL", "Actual AIL", "Forecast AIL & Actual AIL Difference"
        ]
        
        for col in numeric_columns:
            # Remove commas and spaces before converting to numeric
            df[col] = df[col].replace({',': '', ' ': ''}, regex=True)  # Remove commas and spaces
            df[col] = pd.to_numeric(df[col], errors='coerce')  # Convert to numeric, invalid entries become NaN

        # Log how many rows were cleaned for numeric columns
        logger.info(f"Rows after numeric conversion: {df[numeric_columns].isna().sum()} NaN values found")
        logger.debug(f"Rows with NaN values after numeric conversion: {df[df[numeric_columns].isna().any(axis=1)]}")

        # Remove rows with NaN values in essential columns (Date and critical numeric columns)
        df = df.dropna(subset=['Date (HE)'] + numeric_columns)

        # Log total rows after cleaning
        logger.info(f"Total rows to process after cleaning: {len(df)}")

        # If batch_size is provided, split the data into batches
        if batch_size:
            batches = [df.iloc[i:i + batch_size] for i in range(0, len(df), batch_size)]
            logger.info(f"Data split into {len(batches)} batches.")
            return batches  # Return the batches as a list of DataFrames
        
        # Return the cleaned and validated DataFrame
        return df
