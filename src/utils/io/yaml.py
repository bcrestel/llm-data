from typing import Any

import yaml


def save_to_yaml(file_name: str, content: Any) -> None:
    with open(file_name, "w") as file:
        yaml.dump(content, file)


def load_from_yaml(file_name: str) -> None:
    with open(file_name, "r") as file:
        content = yaml.safe_load(file)
    return content
