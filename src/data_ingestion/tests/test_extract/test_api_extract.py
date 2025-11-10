import pytest
from unittest.mock import patch, MagicMock
import requests
from requests.exceptions import RequestException
from src.data_ingestion.extract.api_extract import RedditExtractor 

@pytest.fixture
def extractor_instance():
    """Creates an instance of the RedditExtractor class with test data."""
    return RedditExtractor(
        subreddit="test_subreddit",
        access_token="mock_token_123",
        user_agent="TestScript/1.0"
    )


@patch("src.data_ingestion.extract.api_extract.requests.get")
def test_fetch_threads_success(mock_get, extractor_instance):
    """Tests the success case (status 200) and correct JSON extraction."""
    
    
    mock_response = MagicMock()
    mock_response.status_code = 200
    expected_json = {
        "kind": "Listing",
        "data": {
            "dist": 2,
            "children": [
                {
                    "kind": "t3_testpost1",
                    "data": {
                        "subreddit": "test_subreddit",
                        "author_fullname": "t2_testauthor1",
                        "title": "Test Post 1",
                        "selftext": "This is a test post content.",
                    }
                },
                {
                    "kind": "t3_testpost2",
                    "data": {
                        "subreddit": "test_subreddit",
                        "author_fullname": "t2_testauthor2",
                        "title": "Test Post 2",
                        "selftext": "This is a test post content.",
                    }
                }
            ]
        }
    }
    
    mock_response.json.return_value = expected_json
    mock_get.return_value = mock_response

    result = extractor_instance.fetch_threads(limit=2)

    assert result == expected_json
    expected_url = "https://oauth.reddit.com/r/test_subreddit/new"
    expected_headers = {
        'Authorization': 'bearer mock_token_123',
        'User-Agent': 'TestScript/1.0'
    }
    
    mock_get.assert_called_once_with(
        expected_url,
        headers=expected_headers,
        params={'limit': 2}
    )



@patch("src.data_ingestion.extract.api_extract.requests.get")
def test_fetch_threads_http_error(mock_get, extractor_instance):
    """Tests the case where the API returns an HTTP error (e.g., 403 Forbidden)."""
    
    mock_response = MagicMock()
    mock_status = 403
    mock_error_message = "Forbidden: Invalid token"
    
    mock_response.status_code = mock_status
    mock_response.text = mock_error_message
    mock_get.return_value = mock_response

    result = extractor_instance.fetch_threads(limit=5)

    assert result == {"error": mock_status, "message": mock_error_message}
    
    mock_get.assert_called_once()
    assert mock_get.call_args[1]['params']['limit'] == 5