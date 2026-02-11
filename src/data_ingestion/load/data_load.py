import json
from src.data_ingestion.utils.logger import get_logger

logger = get_logger(__name__)

logger.critical('Hello World, making new module...')

def save_json(data: dict, file_path: str, pretty_print: bool = False) -> None:
    """
    Serializes and saves a dictionary to a JSON file.

    This utility function handles the file writing process, allowing for either 
    a human-readable (indented) or a production-ready (compact) format. It 
    uses UTF-8 encoding and ensures non-ASCII characters are preserved.

    Args:
        data (dict): The dictionary object to be serialized.
        file_path (str): The absolute or relative destination path, including 
            the filename (e.g., 'data/output.json').
        pretty_print (bool, optional): If True, the JSON will be saved with 
            a 4-space indentation. If False, it will be saved in a compact 
            single-line format. Defaults to False.
    """

    indent_value = 4 if pretty_print else None
    separators_value = None if pretty_print else (',', ':')

    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(
            data, 
            f, 
            ensure_ascii=False, 
            indent=indent_value, 
            separators=separators_value
        )
