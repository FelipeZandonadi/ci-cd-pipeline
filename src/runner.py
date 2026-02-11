from src.data_ingestion.config.settings import RedditConfig, PostgresConfig
from src.data_ingestion.utils.logger import get_logger
from src.data_ingestion.extract.api_extract import RedditExtractor
from src.data_ingestion.load.data_load import save_json

logger = get_logger(__name__)

def config_env() -> dict[str, dict[str, str]]:
    reddit_config: RedditConfig = RedditConfig()
    postgres_config: PostgresConfig = PostgresConfig()
    
    return {
        "reddit": reddit_config.env,
        "postgres": postgres_config.env,
    }

def runner():
    configs = config_env()
    logger.info("Configurations loaded successfully.")

    test: RedditExtractor = RedditExtractor(
        client_id=configs["reddit"]["client_id"],
        client_secret=configs["reddit"]["client_secret"],
        username=configs["reddit"]["username"],
        password=configs["reddit"]["password_account"],
        user_agent=configs["reddit"]["user_agent"],
    )
    
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
    
    
    result: list[dict] = test.bootstrap(
        subreddit=subreddits[0],
        limit=25,
    )

    save_json(result, '/app/data/test.json', True)
    logger.debug('TESTE')
    data = test.sync_next_batch(
        subreddit=subreddits[0],
        fullname='t3_1qwihpf',
        limit=25,
    )

    save_json(data, '/app/data/test2.json', True)


if __name__ == "__main__":
    runner()