import os
from dotenv import load_dotenv
from src.data_ingestion.utils.logger import get_logger
from typing import Final

load_dotenv()

logger = get_logger(__name__)

class RedditConfig:
    client_id: str
    client_secret: str
    password_account: str
    user_agent: str
    username: str
    
    REQUIRED_KEYS: Final[list[str]] = [
        "REDDIT_CLIENT_ID",
        "REDDIT_CLIENT_SECRET",
        "REDDIT_PASSWORD_ACCOUNT",
        "REDDIT_USER_AGENT",
        "REDDIT_USERNAME",
    ]

    def __init__(self):
        self._load_config()
        logger.info("Reddit configuration loaded successfully.")

    def _load_config(self) -> None:
        """
        Loads the environment variables and ensures they are not None.
        (Type Narrowing).
        """
        
        for key in self.REQUIRED_KEYS:
            value = os.getenv(key)
            
            if value is None:
                logger.error(f"{key} is not set in environment variables.")
                raise ValueError(f"{key} is not set in environment variables.")
            
            attribute_name = key.lower().replace("reddit_", "")
            
            setattr(self, attribute_name, value)

    @property
    def env(self) -> dict[str, str]:

        return {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "password_account": self.password_account,
            "user_agent": self.user_agent,
            "username": self.username,
        }


class PostgresConfig():
    
    db: str
    user: str
    password: str
    port: str
    data_path: str
    
    REQUIRED_KEYS: Final[list[str]] = [
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_PORT",
        "POSTGRES_DATA_PATH",
    ]
    
    def __init__(self):
        self._load_config()
        logger.info("Postgres configuration loaded successfully.")
        
    def _load_config(self) -> None:
        """
        Loads the environment variables and ensures they are not None.
        (Type Narrowing).
        """
        
        for key in self.REQUIRED_KEYS:
            value = os.getenv(key)
            
            if value is None:
                logger.error(f"{key} is not set in environment variables.")
                raise ValueError(f"{key} is not set in environment variables.")
            
            attribute_name = key.lower().replace("postgres_", "")
            
            setattr(self, attribute_name, value)
        
    @property
    def env(self) -> dict[str, str]:
        return {
            "POSTGRES_DB": self.db,
            "POSTGRES_USER": self.user,
            "POSTGRES_PASSWORD": self.password,
            "POSTGRES_PORT": self.port,
            "POSTGRES_DATA_PATH": self.data_path,
        }