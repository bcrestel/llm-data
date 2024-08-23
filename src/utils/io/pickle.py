import pickle
from typing import Any


def save_to_pickle(file_name: str, content: Any) -> None:
    """
    Save a file in pickle format

    Args:
        file_name (str): name of the file
        content (Any): file to save
    """
    with open(file_name, "wb") as file:
        pickle.dump(content, file)


def load_from_pickle(file_name: str) -> Any:
    """
    Load a pickle file

    Args:
        file_name (str): path to pickle file to load

    Returns:
        Any: Loaded file
    """
    with open(file_name, "rb") as file:
        content = pickle.load(file)
    return content
