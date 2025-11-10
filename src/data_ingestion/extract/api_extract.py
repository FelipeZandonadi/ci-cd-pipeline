import praw
from praw.models import Submission
import src.data_ingestion.utils.logger as logger_utils

logger = logger_utils.get_logger(__name__)

def extract_thread(id: str, reddit_instance: praw.Reddit) -> Submission:
    """
    Extrai uma thread do Reddit pelo ID usando a instância PRAW fornecida.

    Parâmetros:
    - id: ID da thread do Reddit.
    - reddit_instance: instância autenticada do PRAW.

    Retorna:
    - Objeto Submission representando a thread.
    """
    try:
        submission = reddit_instance.submission(id=id)
        logger.info(f"Thread {id} extraída com sucesso.")
        return submission
    except Exception as e:
        logger.error(f"Erro ao extrair a thread {id}: {e}")
        raise
