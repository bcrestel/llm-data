from typing import Any


def save_to_text(file_name: str, content: Any) -> None:
    with open(file_name, "w") as file:
        file.write(content)
