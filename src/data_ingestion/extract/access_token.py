import requests
from requests.auth import HTTPBasicAuth
from datetime import timedelta
import src.data_ingestion.utils.logger as logger_utils


logger = logger_utils.get_logger(__name__)

class AccessToken:
    """
    A class to build access token for Reddit API using OAuth2.

    Attributes:
        client_id (str): The client ID for Reddit API.
        client_secret (str): The client secret for Reddit API.
        username (str): The Reddit username.
        password (str): The Reddit password.
        headers (dict): The headers to be used in the requests.
    """
    
    def __init__(self, client_id: str, client_secret: str, username: str, password: str, user_agent: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password
        self.headers = {'User-Agent': user_agent}
        
        logger.info("AccessToken initialized.")
    
    @property
    def access_token(self) -> str:
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
            headers=self.headers
            )
        if response.status_code != 200:
            logger.error(f"Failed to obtain access token: {response.text}")
            raise Exception("Failed to obtain access token from Reddit API.")
        
        token = response.json().get('access_token')
        
        logger.info(f"Access token obtained successfully. Expires in {timedelta(seconds=response.json().get('expires_in', 0))}.")
        return token