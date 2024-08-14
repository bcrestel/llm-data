import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional
import yaml

import requests

from src.utils.constant import (
    HELM_MODEL_FILE_PREFIX,
    HELM_MODEL_URL,
    HELM_REPO_MAIN,
    LOCAL_PATH_TO_RAW_DATA,
    PATH_TO_RAW_DATA_LOG,
)
from src.utils.date import get_date_YYYY_MM_DD
from src.utils.git import get_current_git_commit_short
from src.utils.path import (
    change_permission_single_file,
    chmod_from_bottom_to_top,
    chmod_from_top_to_bottom,
    get_shasum,
)
from src.utils.web import get_html_content_from_url
from src.utils.text import save_to_text

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(filename)s--l.%(lineno)d: %(message)s",
)
logger = logging.getLogger(__name__)


class HelmModels:
    def __init__(self, url: str = HELM_MODEL_URL) -> None:
        self.url = url
        self.main_repo_url = HELM_REPO_MAIN

    def get_raw_data(self) -> None:
        """Scrape webpage and save html"""
        # serialize to data/raw
        response = requests.get(self.main_repo_url)
        commit = response.json()["sha"][:7]
        file_name = Path(LOCAL_PATH_TO_RAW_DATA) / self.file_name(
            type="raw", extension="yaml", commit=commit
        )

        logger.info(f"Saving url {self.url} to file {file_name}")
        chmod_from_top_to_bottom(
            LOCAL_PATH_TO_RAW_DATA, PATH_TO_RAW_DATA_LOG, permission=0o744
        )
        html_string = get_html_content_from_url(self.url)
        html_content = yaml.safe_load(html_string) # convert string to proper html
        save_to_text(file_name=file_name, content=html_content)
        with open(PATH_TO_RAW_DATA_LOG, "r") as file:
            data = json.load(file)
        logger.debug(data)
        # Create new entry for json
        shasum = get_shasum(file_name)
        source = f"src/data/helm_models.py--{get_current_git_commit_short()}"
        new_entry = {
            "file_name": f"{file_name}",
            "source": f"{source}",
            "date": f"{get_date_YYYY_MM_DD()}",
            "shasum": f"{shasum}",
        }
        data.append(new_entry)
        logger.debug(data)
        with open(PATH_TO_RAW_DATA_LOG, "w") as file:
            json.dump(data, file, indent=4)
        change_permission_single_file(PATH_TO_RAW_DATA_LOG, permission=0o444)
        chmod_from_bottom_to_top(LOCAL_PATH_TO_RAW_DATA, file_name, permission=0o444)
        change_permission_single_file(LOCAL_PATH_TO_RAW_DATA, permission=0o544)

    @staticmethod
    def file_name(type: str, extension: str, commit: str, date: Optional[datetime] = None) -> Path:
        if date is None:
            now = datetime.now()
            date = now.strftime("%Y-%m-%d")
        return Path(f"{HELM_MODEL_FILE_PREFIX}_{type}_{date}_{commit}.{extension}")


if __name__ == "__main__":
    helm = HelmModels()
    helm.get_raw_data()
