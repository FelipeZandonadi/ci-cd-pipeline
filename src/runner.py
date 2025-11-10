from src.data_ingestion.config.settings import RedditConfig, PostgresConfig
from src.data_ingestion.utils.logger import get_logger

logger = get_logger(__name__)

def config_env():
    praw_config = RedditConfig()
    postgres_config = PostgresConfig()
    
    return {
        "praw": praw_config.env,
        "postgres": postgres_config.env,
    }
    
    
def runner():
    configs = config_env()
    logger.info("Configurations loaded successfully.")
    
    
