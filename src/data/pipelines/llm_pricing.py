import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests

from src.utils.constant import (
    LLMPRICING_API,
    LLMPRICING_FILE_PREFIX,
    LLMPRICING_URL,
    LOCAL_PATH_TO_RAW_DATA,
)
from src.utils.git import get_current_git_commit_short
from src.utils.io.protected_folder import ProtectedFolder
from src.utils.io.text import save_to_text
from src.utils.web import get_html_content_from_url

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(filename)s--l.%(lineno)d: %(message)s",
)
logger = logging.getLogger(__name__)


class LLMPricing:
    def __init__(self, url: str = LLMPRICING_URL) -> None:
        self.url = url
        self.api_path = LLMPRICING_API

    def get_raw_data(self) -> None:
        response = requests.get(self.main_repo_url)
        commit = response.json()["sha"][:7]
        file_name = Path(LOCAL_PATH_TO_RAW_DATA) / self.file_name(
            type="raw", extension="ts", commit=commit
        )

        logger.info(f"Saving url {self.url} to file {file_name}")
        html_content = get_html_content_from_url(self.url)
        folder = ProtectedFolder(
            root_folder=LOCAL_PATH_TO_RAW_DATA, log_name="raw_data_log.json"
        )
        source = f"{Path(__file__)}--{get_current_git_commit_short()}"
        folder.save_file(
            save_function=save_to_text,
            parameters={"file_name": file_name, "content": html_content},
            source=source,
        )

    @staticmethod
    def file_name(
        type: str, extension: str, commit: str = "", date: Optional[datetime] = None
    ) -> Path:
        if date is None:
            now = datetime.now()
            date = now.strftime("%Y-%m-%d")
        return Path(f"{LLMPRICING_FILE_PREFIX}_{type}_{date}_{commit}.{extension}")


if __name__ == "__main__":
    llmpricing = LLMPricing()
    llmpricing.get_raw_data()
