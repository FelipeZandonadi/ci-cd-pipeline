import pytest
from data_ingestion.extract.reddit import RedditAuth, RedditExtractor
from requests.auth import HTTPBasicAuth


# ==========================================
# ---------- Tests for RedditAuth ----------
# ==========================================

@pytest.fixture
def auth_service():
    return RedditAuth(
        client_id="mock_client_id",
        client_secret="mock_client_secret",
        username="mock_username",
        password="mock_password",
        user_agent="mock_user_agent",
    )

@pytest.fixture
def mock_requests_post(mocker):
    mock_response = mocker.Mock()
    mock_post = mocker.patch('data_ingestion.extract.reddit.requests.post', return_value=mock_response)

    return mock_post, mock_response

def test_reddit_auth_success(auth_service, mock_requests_post):
    mock_post, mock_response = mock_requests_post
    mock_response.status_code = 200
    mock_response.json.return_value = {"access_token": "mock_token_123", "expires_in": 3600}

    token = auth_service.access_token()
    assert token == "mock_token_123"
    auth = HTTPBasicAuth("mock_client_id", "mock_client_secret")
    mock_post.assert_called_once_with(
        "https://www.reddit.com/api/v1/access_token",
        auth=auth,
        data={
            "grant_type": "password",
            "username": "mock_username",
            "password": "mock_password",
        },
        headers={"User-Agent": "mock_user_agent"},
    )


def test_reddit_auth_failure(auth_service, mock_requests_post):
    mock_post, mock_response = mock_requests_post
    mock_response.status_code = 401
    mock_response.json.return_value = {"error": "invalid_grant"}

    with pytest.raises(Exception) as exc_info:
        auth_service.access_token()
    assert "[401] Failed to obtain access token from Reddit API." in str(exc_info.value)
    auth = HTTPBasicAuth("mock_client_id", "mock_client_secret")
    mock_post.assert_called_once_with(
        "https://www.reddit.com/api/v1/access_token",
        auth=auth,
        data={
            "grant_type": "password",
            "username": "mock_username",
            "password": "mock_password",    },
        headers={"User-Agent": "mock_user_agent"},
    )

def test_reddit_auth_no_token(auth_service, mock_requests_post):
    mock_post, mock_response = mock_requests_post
    mock_response.status_code = 200
    mock_response.json.return_value = {"error": "invalid_grant"}

    with pytest.raises(Exception) as exc_info:
        auth_service.access_token()
    assert "Access token not found in Reddit API response." in str(exc_info.value)
    auth = HTTPBasicAuth("mock_client_id", "mock_client_secret")
    mock_post.assert_called_once_with(
        "https://www.reddit.com/api/v1/access_token",
        auth=auth,
        data={
            "grant_type": "password",
            "username": "mock_username",
            "password": "mock_password",
        },
        headers={"User-Agent": "mock_user_agent"},
    )


# ===============================================
# ---------- Tests for RedditExtractor ----------
# ===============================================

@pytest.fixture
def mock_requests_get(mocker):
    mock_response = mocker.Mock()
    mock_get = mocker.patch('data_ingestion.extract.reddit.requests.get', return_value=mock_response)

    return mock_get, mock_response

def test_reddit_extractor_fetch_thread_before(mock_requests_get):
    mock_get, mock_response = mock_requests_get
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "data": {
            "children": [
                {"data": {"name": "thread1", "created_utc": 1700000000}},
                {"data": {"name": "thread2", "created_utc": 1700001000}},
            ]
        }
    }

    extractor = RedditExtractor(token="mock_token_123", user_agent="mock_user_agent")
    threads = extractor.fetch_thread_before(subreddit="mock_subreddit", fullname="t3_12345", limit=2)

    assert len(threads) == 1
    assert threads["data"]["children"][0]["data"]["name"] == "thread1"

    mock_get.assert_called_once_with(
        "https://oauth.reddit.com/r/mock_subreddit/new",
        headers={"Authorization": "bearer mock_token_123", "User-Agent": "mock_user_agent"},
        params={"before": "t3_12345", "limit": 2},
    )

def test_reddit_extractor_fetch_thread_before_failure(mock_requests_get):
    mock_get, mock_response = mock_requests_get
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"

    extractor = RedditExtractor(token="mock_token_123", user_agent="mock_user_agent")

    with pytest.raises(Exception) as exc_info:
        extractor.fetch_thread_before(subreddit="mock_subreddit", fullname="t3_12345", limit=2)
    assert "[500] Failed to fetch thread from subreddit: mock_subreddit" in str(exc_info.value)

    mock_get.assert_called_once_with(
        "https://oauth.reddit.com/r/mock_subreddit/new",
        headers={"Authorization": "bearer mock_token_123", "User-Agent": "mock_user_agent"},
        params={"before": "t3_12345", "limit": 2},
    )


