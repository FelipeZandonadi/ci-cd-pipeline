from src.data_ingestion.config.settings import RedditConfig, PostgresConfig
from src.data_ingestion.utils.logger import get_logger
from src.data_ingestion.extract.access_token import AccessToken
from src.data_ingestion.extract.api_extract import RedditExtractor
from datetime import date

logger = get_logger(__name__)

def config_env() -> dict[str, dict[str, str]]:
    reddit_config: RedditConfig = RedditConfig()
    postgres_config: PostgresConfig = PostgresConfig()
    
    return {
        "reddit": reddit_config.env,
        "postgres": postgres_config.env,
    }
    
def generate_reddit_token(configs) -> str:
    
    acessToken: AccessToken = AccessToken(
        client_id=configs["reddit"]["client_id"],
        client_secret=configs["reddit"]["client_secret"],
        username=configs["reddit"]["username"],
        password=configs["reddit"]["password_account"],
        user_agent=configs["reddit"]["user_agent"],
        )
    
    token: str = acessToken.access_token
    return token

def runner():
    configs = config_env()
    logger.info("Configurations loaded successfully.")

    token: str = generate_reddit_token(configs)
    logger.info(f"Generated Reddit Token: {token[0:4]}...{token[-4:]}")
    
    subreddits: list[str] = [
        "CryptoCurrency",
        "Bitcoin",
        "Ethereum",
        "ethtrader",
        "dogecoin",
        "CryptoMoonshots",
        "btc",
        "BitcoinBeginners",
        "CryptoTechnology",
    ]
    
    test = RedditExtractor(
        subreddit=subreddits[0],
        access_token=token,
        user_agent=configs["reddit"]["user_agent"]
    )
    
    print(test.fetch_threads(limit=5))