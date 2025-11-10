import os
from dotenv import load_dotenv
from src.data_ingestion.utils.logger import get_logger

load_dotenv()

logger = get_logger(__name__)

class RedditConfig():
    def __init__(self):
        self.__REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
        self.__REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
        self.__REDDIT_PASSWORD_ACCOUNT = os.getenv("REDDIT_PASSWORD_ACCOUNT")
        self.__REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT")
        self.__REDDIT_USERNAME = os.getenv("REDDIT_USERNAME")
        
        if self.__REDDIT_CLIENT_ID is None:
            logger.error("REDDIT_CLIENT_ID is not set in environment variables.")
            raise ValueError("REDDIT_CLIENT_ID is not set in environment variables.")
        if self.__REDDIT_CLIENT_SECRET is None:
            logger.error("REDDIT_CLIENT_SECRET is not set in environment variables.")
            raise ValueError("REDDIT_CLIENT_SECRET is not set in environment variables.")
        if self.__REDDIT_PASSWORD_ACCOUNT is None:
            logger.error("REDDIT_PASSWORD_ACCOUNT is not set in environment variables.")
            raise ValueError("REDDIT_PASSWORD_ACCOUNT is not set in environment variables.")
        if self.__REDDIT_USER_AGENT is None:
            logger.error("REDDIT_USER_AGENT is not set in environment variables.")
            raise ValueError("REDDIT_USER_AGENT is not set in environment variables.")
        if self.__REDDIT_USERNAME is None:
            logger.error("REDDIT_USERNAME is not set in environment variables.")
            raise ValueError("REDDIT_USERNAME is not set in environment variables.")
        
        logger.info("REDDIT configuration loaded successfully.")
        

    @property
    def env(self):
        return {
            "REDDIT_CLIENT_ID": self.__REDDIT_CLIENT_ID,
            "REDDIT_CLIENT_SECRET": self.__REDDIT_CLIENT_SECRET,
            "REDDIT_PASSWORD_ACCOUNT": self.__REDDIT_PASSWORD_ACCOUNT,
            "REDDIT_USER_AGENT": self.__REDDIT_USER_AGENT,
            "REDDIT_USERNAME": self.__REDDIT_USERNAME,
        }


class PostgresConfig():
    def __init__(self):
        self.__POSTGRES_DB = os.getenv("POSTGRES_DB")
        self.__POSTGRES_USER = os.getenv("POSTGRES_USER")
        self.__POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
        self.__POSTGRES_PORT = os.getenv("POSTGRES_PORT")
        self.__POSTGRES_DATA_PATH = os.getenv("POSTGRES_DATA_PATH")

        if self.__POSTGRES_DB is None:
            logger.error("POSTGRES_DB is not set in environment variables.")
            raise ValueError("POSTGRES_DB is not set in environment variables.")
        if self.__POSTGRES_USER is None:
            logger.error("POSTGRES_USER is not set in environment variables.")
            raise ValueError("POSTGRES_USER is not set in environment variables.")
        if self.__POSTGRES_PASSWORD is None:
            logger.error("POSTGRES_PASSWORD is not set in environment variables.")
            raise ValueError("POSTGRES_PASSWORD is not set in environment variables.")
        if self.__POSTGRES_PORT is None:
            logger.error("POSTGRES_PORT is not set in environment variables.")
            raise ValueError("POSTGRES_PORT is not set in environment variables.")
        if self.__POSTGRES_DATA_PATH is None:
            logger.error("POSTGRES_DATA_PATH is not set in environment variables.")
            raise ValueError("POSTGRES_DATA_PATH is not set in environment variables.")
        
        logger.info("Postgres configuration loaded successfully.")
        
    @property
    def env(self):
        return {
            "POSTGRES_DB": self.__POSTGRES_DB,
            "POSTGRES_USER": self.__POSTGRES_USER,
            "POSTGRES_PASSWORD": self.__POSTGRES_PASSWORD,
            "POSTGRES_PORT": self.__POSTGRES_PORT,
            "POSTGRES_DATA_PATH": self.__POSTGRES_DATA_PATH,
        }