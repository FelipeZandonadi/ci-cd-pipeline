import requests
from requests.auth import HTTPBasicAuth
from datetime import timedelta
from src.data_ingestion.utils.logger import get_logger

logger = get_logger(__name__)

class RedditExtractor:
    """
    A class to extract data raw (the format is json) from Subreddit using the requests library.

    Attributes:
        client_id (str): The client ID for Reddit API.
        client_secret (str): The client secret for Reddit API.
        username (str): The Reddit username.
        password (str): The Reddit password.
        user_agent (str): The application name
    """
    
    base_url: str
    client_id: str
    client_secret: str
    username: str
    password: str
    user_agent: str
    headers: dict[str, str]
    
    def __init__(self, client_id: str, client_secret: str, username: str, password: str, user_agent: str):
        self.base_url = "https://oauth.reddit.com"
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password
        self.user_agent = user_agent

        def access_token() -> str:
            """
            Build access token for Reddit API using OAuth2.

            Returns:
                str: The access token.
            """
            auth: HTTPBasicAuth = HTTPBasicAuth(self.client_id, self.client_secret)
            data: dict[str, str] = {
                'grant_type': 'password',
                'username': self.username,
                'password': self.password
            }
            response = requests.post(
                'https://www.reddit.com/api/v1/access_token',
                auth=auth,
                data=data,
                headers={'User-Agent': user_agent}
                )
            if response.status_code != 200:
                logger.error(f"Failed to obtain access token: {response.text}")
                raise Exception("Failed to obtain access token from Reddit API.")
            
            token = response.json().get('access_token')
            
            logger.info(f"Access token obtained successfully. Expires in {timedelta(seconds=response.json().get('expires_in', 0))}.")
            return token

        token = access_token()

        self.headers = {
            'Authorization': f'bearer {token}',
            'User-Agent': self.user_agent
        }
        logger.info(f"RedditExtractor initialized")


    def fetch_threads(self, subreddit, limit: int = 10) -> dict:
        thread_endpoint = f"/r/{subreddit}/new"
        url = f"{self.base_url}{thread_endpoint}"
        params = {
            'limit': limit
        }    
        response = requests.get(url, headers=self.headers, params=params)
        
        if response.status_code == 200:
            logger.info(f"Fetched threads successfully from subreddit: {subreddit}")
            return response.json()
        else:
            logger.error(f"Failed to fetch threads from subreddit: {subreddit}")
            return {"error": response.status_code, "message": response.text}
        
    def fetch_comments(self, subreddit) -> None:
        comments_endpoint = f"#"