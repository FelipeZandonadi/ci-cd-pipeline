import requests
from requests.auth import HTTPBasicAuth
import src.data_ingestion.utils.logger as logger_utils

logger = logger_utils.get_logger(__name__)

class RedditExtractor:
    """
    A class to extract data raw (the format is json) from Subreddit using the requests library.

    Attributes:
        base_url (str): The base URL for Reddit API.
        thread_endpoint (str): The endpoint for fetching subreddit threads.
        comments_endpoint (str): The endpoint for fetching comments of a thread.
        subreddit (str): The subreddit to extract data from.
        headers (dict): The headers to be used in the requests.
        access_token (str): The access token for Reddit API authentication.
    """
    
    base_url: str
    thread_endpoint: str
    comments_endpoint: str
    subreddit: str
    headers: dict[str, str]
    access_token: str
    user_agent: str
    
    def __init__(self, subreddit: str, access_token: str, user_agent: str):
        self.base_url = "https://oauth.reddit.com"
        self.thread_endpoint = f"/r/{subreddit}/new"
        self.comments_endpoint = f"#"
        self.subreddit = subreddit
        self.access_token = access_token
        self.user_agent = user_agent
        self.headers = {
            'Authorization': f'bearer {self.access_token}',
            'User-Agent': self.user_agent
        }
        logger.info(f"RedditExtractor initialized for subreddit: {self.subreddit}")


    def fetch_threads(self, limit: int = 10) -> dict:
        url = f"{self.base_url}{self.thread_endpoint}"
        params = {
            'limit': limit
        }    
        response = requests.get(url, headers=self.headers, params=params)
        
        if response.status_code == 200:
            logger.info(f"Fetched threads successfully from subreddit: {self.subreddit}")
            return response.json()
        else:
            logger.error(f"Failed to fetch threads from subreddit: {self.subreddit}")
            return {"error": response.status_code, "message": response.text}