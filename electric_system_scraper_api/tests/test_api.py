from fastapi.testclient import TestClient
from app.api import app
import pytest

# Initialize TestClient for the FastAPI app
client = TestClient(app)

def test_root_endpoint():
    """
    Test the root endpoint to ensure it returns the expected welcome message.
    """
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Welcome to the API! Use /scrape, /fetch-processed-data, /aggregated-data, or /fetch-raw-data."}

def test_scrape_endpoint(mocker):
    """
    Test the /scrape endpoint to ensure it successfully processes data.
    """
    # Mock the fetch operation to return dummy HTML
    mocker.patch("app.scraper.fetcher.Fetcher.fetch", return_value="<html><table><tr><th>Header</th></tr></table></html>")
    # Mock the parse operation to return predefined headers and rows
    mocker.patch("app.scraper.parser.Parser.parse_table", return_value=(["Header"], [["Data"]]))
    # Mock the transform operation to return an empty generator
    mocker.patch("app.scraper.transformer.Transformer.transform", return_value=iter([]))
    
    # Mock the database operation (rollback and query)
    db_mock = mocker.patch("app.database.operations.DatabaseOperations")
    db_instance = db_mock.return_value
    db_instance.rollback = mocker.Mock()
    db_instance.query = mocker.Mock()
    db_instance.query.return_value.all.return_value = []  # Return an empty list for query

    # Call the endpoint
    response = client.get("/scrape")
    assert response.status_code == 200
    assert response.json()["status"] == "success"

def test_fetch_processed_data_endpoint(mocker):
    """
    Test the /fetch-processed-data endpoint to ensure it returns the correct processed data.
    """
    # Mock the fetch operation to return dummy HTML
    mocker.patch("app.scraper.fetcher.Fetcher.fetch", return_value="<html><table><tr><th>Header</th></tr></table></html>")
    # Mock the parse operation to return predefined headers and rows
    mocker.patch("app.scraper.parser.Parser.parse_table", return_value=(["Header"], [["Data"]]))
    # Mock the transform operation to return processed data
    mocker.patch("app.scraper.transformer.Transformer.transform", return_value=[["Processed Data"]])
    
    # Mock the database operation (rollback)
    db_mock = mocker.patch("app.database.operations.DatabaseOperations")
    db_instance = db_mock.return_value
    db_instance.rollback = mocker.Mock()

    # Call the endpoint
    response = client.get("/fetch-processed-data")
    assert response.status_code == 200
    assert response.json()["status"] == "success"
    assert response.json()["data"] == [["Processed Data"]]

def test_aggregated_data_endpoint(mocker):
    """
    Test the /aggregated-data endpoint to ensure it performs aggregation correctly.
    """
    # Mock the fetch operation to return dummy HTML
    mocker.patch("app.scraper.fetcher.Fetcher.fetch", return_value="<html><table><tr><th>Header</th></tr></table></html>")
    # Mock the parse operation to return predefined headers and rows
    mocker.patch("app.scraper.parser.Parser.parse_table", return_value=(["Header"], [["Data"]]))
    # Mock the transform operation to return transformed data
    mocker.patch("app.scraper.transformer.Transformer.transform", return_value=[["Transformed Data"]])
    
    # Create a mock request body for aggregation
    request_body = {"aggregation_type": "monthly"}
    
    # Mock the database operation (rollback)
    db_mock = mocker.patch("app.database.operations.DatabaseOperations")
    db_instance = db_mock.return_value
    db_instance.rollback = mocker.Mock()

    # Call the endpoint
    response = client.post("/aggregated-data", json=request_body)
    assert response.status_code == 200
    assert response.json()["status"] == "success"
    assert response.json()["aggregated_data"] == [["Transformed Data"]]

def test_invalid_aggregated_data_endpoint(mocker):
    """
    Test the /aggregated-data endpoint with invalid aggregation type.
    """
    # Create a mock request body for an unsupported aggregation type
    request_body = {"aggregation_type": "invalid"}
    
    # Call the endpoint
    response = client.post("/aggregated-data", json=request_body)
    assert response.status_code == 200
    assert response.json()["status"] == "error"
    assert response.json()["message"] == "Unsupported aggregation type"
