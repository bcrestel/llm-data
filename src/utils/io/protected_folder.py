from pathlib import Path
import json
from src.utils.path import get_shasum, chmod_from_bottom_to_top, change_permission_single_file, chmod_from_top_to_bottom
from src.utils.date import get_date_YYYY_MM_DD
import logging
from typing import Optional

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(filename)s--l.%(lineno)d: %(message)s",
)
logger = logging.getLogger(__name__)


class ProtectedFolder:
    def __init__(self, root_folder: str, log_name: str = "log.json") -> None:
        self.root_folder = Path(root_folder)
        self.log_name = log_name

    def save_file(self, save_function: callable, parameters: dict, source: str = "", file_name: Optional[str] = None) -> None:
        if file_name is None:
            file_name = parameters["file_name"]
        file_name = Path(file_name)
        chmod_from_top_to_bottom(self.root_folder, file_name.parent, permission=0o744)
        save_function(**parameters)
        log_path = self.add_entry_to_log(file_name=file_name, source=source)
        change_permission_single_file(file_name, permission=0o444)
        chmod_from_bottom_to_top(self.root_folder, log_path, permission=0o544)
    
    def add_entry_to_log(self, file_name: Path, source: str) -> Path:
        log_path = file_name.parent / self.log_name

        if log_path.exists():
            with open(log_path, "r") as file:
                data = json.load(file)
        else:
            data = []
        logger.debug(data)

        # Create new entry for json
        shasum = get_shasum(file_name)
        new_entry = {
            "file_name": f"{file_name}",
            "source": f"{source}",
            "date": f"{get_date_YYYY_MM_DD()}",
            "shasum": f"{shasum}",
        }
        data.append(new_entry)
        logger.debug(data)
        with open(log_path, "w") as file:
            json.dump(data, file, indent=4)
        
        return log_path