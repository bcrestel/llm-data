import logging
from datetime import datetime
from pathlib import Path
from typing import Optional
import json

from src.utils.constant import (
    HELM_LEADERBOARD_FILE_PREFIX,
    HELM_MODEL_URL,
    LOCAL_PATH_TO_RAW_DATA,
    PATH_TO_RAW_DATA_LOG
)
from src.utils.pickle import save_to_pickle
from src.utils.web import get_html_content_from_url
from src.utils.path import chmod_from_bottom_to_top, chmod_from_top_to_bottom, get_shasum, change_permission_single_file
from src.utils.git import get_current_git_commit_short
from src.utils.date import get_date_YYYY_MM_DD

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(filename)s--l.%(lineno)d: %(message)s",
)
logger = logging.getLogger(__name__)


class HelmModels:
    def __init__(self, url: str = HELM_MODEL_URL) -> None:
        self.url = url

    def get_raw_data(self) -> None:
        """Scrape webpage and save html"""
        html_content = get_html_content_from_url(self.url)
        # serialize to data/raw
        file_name = Path(LOCAL_PATH_TO_RAW_DATA) / self.file_name(
            type="raw", extension="pickle"
        )
        logger.info(f"Saving url {self.url} to file {file_name}")
        chmod_from_top_to_bottom(LOCAL_PATH_TO_RAW_DATA, PATH_TO_RAW_DATA_LOG, permission=0o744)
        save_to_pickle(file_name=file_name, content=html_content)
        with open(PATH_TO_RAW_DATA_LOG, 'r') as file:
            data = json.load(file)
        logger.debug(data)
        # Create new entry for json
        shasum = get_shasum(file_name)
        source = f"src/data/helm_models.py--{get_current_git_commit_short()}"
        new_entry = {"file_name": f"{file_name}", "source": f"{source}", "date": f"{get_date_YYYY_MM_DD()}", "shasum": f"{shasum}"}
        data.append(new_entry)
        logger.debug(data)
        with open(PATH_TO_RAW_DATA_LOG, 'w') as file:
            json.dump(data, file, indent=4)
        change_permission_single_file(PATH_TO_RAW_DATA_LOG, permission=0o444)
        chmod_from_bottom_to_top(LOCAL_PATH_TO_RAW_DATA, file_name, permission=0o444)

    @staticmethod
    def file_name(type: str, extension: str, date: Optional[datetime] = None) -> Path:
        if date is None:
            now = datetime.now()
            date = now.strftime("%Y-%m-%d")
        return Path(f"{HELM_LEADERBOARD_FILE_PREFIX}_{type}_{date}.{extension}")


if __name__ == "__main__":
    helm = HelmModels()
    helm.get_raw_data()
