import pytest
from unittest.mock import patch, MagicMock
from requests.auth import HTTPBasicAuth
from src.data_ingestion.extract.access_token import AccessToken

@pytest.fixture
def access_token_instance():
    return AccessToken(
        client_id="test_client_id",
        client_secret="test_client_secret",
        username="test_username",
        password="test_password",
        user_agent="test_user_agent"
    )

@patch("src.data_ingestion.extract.access_token.requests.post")
def test_access_token_success(mock_post, access_token_instance):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "access_token": "mock_access_token",
        "expires_in": 3600
    }
    mock_post.return_value = mock_response

    token = access_token_instance.access_token

    assert token == "mock_access_token"
    mock_post.assert_called_once_with(
        'https://www.reddit.com/api/v1/access_token',
        auth=HTTPBasicAuth("test_client_id", "test_client_secret"),
        data={
            'grant_type': 'password',
            'username': "test_username",
            'password': "test_password"
        },
        headers={'User-Agent': "test_user_agent"}
    )

@patch("src.data_ingestion.extract.access_token.requests.post")
def test_access_token_failure(mock_post, access_token_instance):
    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.text = "Bad Request"
    mock_post.return_value = mock_response

    with pytest.raises(Exception, match="Failed to obtain access token from Reddit API."):
        access_token_instance.access_token

    mock_post.assert_called_once_with(
        'https://www.reddit.com/api/v1/access_token',
        auth=HTTPBasicAuth("test_client_id", "test_client_secret"),
        data={
            'grant_type': 'password',
            'username': "test_username",
            'password': "test_password"
        },
        headers={'User-Agent': "test_user_agent"}
    )