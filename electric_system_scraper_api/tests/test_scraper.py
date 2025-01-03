import pytest
from app.scraper.parser import Parser
from app.scraper.fetcher import Fetcher
from requests.exceptions import RequestException

# Mock URL for testing
MOCK_URL = "http://example.com/mock"

def test_fetcher_success(mocker):
    """
    Test that the Fetcher class successfully fetches data from a URL.
    """
    # Mock a successful fetch
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.text = "<html><table><tr><th>Header</th></tr><tr><td>Data</td></tr></table></html>"
    mocker.patch("requests.get", return_value=mock_response)

    fetcher = Fetcher(MOCK_URL)
    response = fetcher.fetch()
    assert response == mock_response.text

def test_fetcher_failure(mocker):
    """
    Test that the Fetcher class raises an exception on network failure.
    """
    # Mock a failed fetch
    mocker.patch("requests.get", side_effect=RequestException("Network Error"))
    fetcher = Fetcher(MOCK_URL)

    with pytest.raises(RequestException):
        fetcher.fetch()

def test_parser_empty_html():
    """
    Test the Parser class with empty HTML content.
    """
    parser = Parser()
    headers, rows = parser.parse_table("")
    assert headers == []
    assert rows == []

def test_parser_valid_html():
    """
    Test the Parser class with valid HTML content containing a table.
    """
    parser = Parser()
    html_content = '<html><table border="1"><tr><th>Header1</th><th>Header2</th></tr><tr><td>Row1Col1</td><td>Row1Col2</td></tr></table></html>'
    headers, rows = parser.parse_table(html_content)
    assert headers == ["Header1", "Header2"]
    assert rows == [["Row1Col1", "Row1Col2"]]

def test_parser_no_table():
    """
    Test the Parser class when there is no table in the HTML content.
    """
    parser = Parser()
    html_content = "<html><body><p>No table here!</p></body></html>"
    headers, rows = parser.parse_table(html_content)
    assert headers == []
    assert rows == []
