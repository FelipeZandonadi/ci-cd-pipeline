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
    
    def __init__(self, subreddit: str, user_agent: str):
        pass
