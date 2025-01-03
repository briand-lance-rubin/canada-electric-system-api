import pytest
import pandas as pd
from app.scraper.transformer import Transformer

def test_transformer_valid_data():
    """
    Test that the Transformer class processes valid data correctly.
    """
    headers = ["Date (HE)", "Actual Posted Pool Price", "Forecast Pool Price"]
    rows = [["12/30/2024 01", "20.5", "22.3"], ["12/30/2024 02", "21.0", "23.0"]]
    transformer = Transformer()
    batch_generator = transformer.transform(headers, rows)

    # Collect all batches
    batches = list(batch_generator)
    assert len(batches) == 1  # Only one batch since data is small
    assert len(batches[0]) == 2  # Two valid rows

def test_transformer_invalid_data():
    """
    Test that the Transformer class handles invalid data gracefully.
    """
    headers = ["Date (HE)", "Actual Posted Pool Price", "Forecast Pool Price"]
    rows = [["INVALID_DATE", "INVALID_VALUE", "INVALID_VALUE"]]
    transformer = Transformer()
    batch_generator = transformer.transform(headers, rows)

    # Collect all batches
    batches = list(batch_generator)
    assert len(batches) == 0  # All rows are invalid
