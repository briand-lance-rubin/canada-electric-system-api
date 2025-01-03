import pytest
import pandas as pd
from app.database.models import HistoricalDemand
from app.database.operations import DatabaseOperations

@pytest.fixture
def db_operations(mocker):
    """
    Fixture to initialize DatabaseOperations for testing.
    """
    db_operations = DatabaseOperations()
    db_operations.db = mocker.Mock()  # Mock the database session
    return db_operations

def test_fetch_all_data_empty(db_operations, mocker):
    """
    Test that the DatabaseOperations class handles an empty database gracefully.
    """
    # Mock the query method to return an empty list
    db_operations.db.query.return_value.all.return_value = []

    # Fetch data and validate
    data = db_operations.fetch_all_data()
    assert data.empty
