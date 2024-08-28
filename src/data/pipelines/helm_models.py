import glob
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
import requests

from src.utils.constant import (
    HELM_MODEL_FILE_PREFIX,
    HELM_MODEL_URL,
    HELM_REPO_MAIN,
    LOCAL_PATH_TO_INT_DATA,
    LOCAL_PATH_TO_RAW_DATA,
)
from src.utils.git import get_current_git_commit_short
from src.utils.io.protected_folder import ProtectedFolder
from src.utils.io.text import save_to_text
from src.utils.io.yaml import load_from_yaml
from src.utils.web import get_html_content_from_url

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
        html_content = get_html_content_from_url(self.url)
        folder = ProtectedFolder(
            root_folder=LOCAL_PATH_TO_RAW_DATA, log_name="raw_data_log.json"
        )
        source = f"src/data/helm_models.py--{get_current_git_commit_short()}"
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
        return Path(f"{HELM_MODEL_FILE_PREFIX}_{type}_{date}_{commit}.{extension}")

    def get_intermediate_from_raw(self, raw_filename: Optional[str] = None):
        if raw_filename is None:
            # if no file_name provided, select the most recent raw file
            file_pattern = (
                Path(LOCAL_PATH_TO_RAW_DATA) / f"{HELM_MODEL_FILE_PREFIX}_*.yaml"
            )
            file_names = {}
            for ff in glob.glob(str(file_pattern)):
                _, date, _ = self.get_type_date_from_path(ff)
                file_names[pd.to_datetime(date)] = ff
            date = min(file_names.keys())
            raw_filename = file_names[date]
        _, date, commit = self.get_type_date_from_path(raw_filename)
        raw_content = load_from_yaml(raw_filename)
        df_helm = pd.DataFrame(raw_content["models"])

        # Clean-up
        # Drop dummy model + fat-finger column
        df_helm = (
            df_helm.set_index("name")
            .drop("simple/model1")
            .drop("tafs", axis=1)
            .reset_index()
        )
        # Replace NaN entries in 'tags' column with an empty list
        df_helm["tags"] = df_helm["tags"].apply(
            lambda x: x if isinstance(x, list) else []
        )

        # Add a short_name column, w/o the org name
        df_helm["short_name"] = df_helm["name"].str.split("/", expand=True)[1]
        # Create tags one-hot encoding:
        df_tags_onehot = (
            df_helm["tags"].apply(lambda x: pd.Series(1, index=x)).fillna(0).astype(int)
        )
        df_helm = pd.concat([df_helm, df_tags_onehot], axis=1).drop(columns="tags")

        # Typing
        df_helm["release_date"] = pd.to_datetime(df_helm["release_date"])

        # Parse Description columns
        df_tmp = (
            df_helm["description"]
            .apply(self.truncate_description)
            .apply(pd.Series)
            .rename(columns={0: "description", 1: "documentation"})
        )
        df_helm.drop(columns="description", inplace=True)
        df_helm = pd.concat([df_helm, df_tmp], axis=1)

        logger.debug(df_helm)
        # Save joined table in 02_intermediate folder
        output_file_path = Path(LOCAL_PATH_TO_INT_DATA) / self.file_name(
            type="intermediate", extension="parquet", commit=commit, date=date
        )
        df_helm.to_parquet(path=output_file_path)
        logger.info(f"Saved formatted Dataframe to {output_file_path}")

    @staticmethod
    def tag_columns(df: pd.DataFrame) -> List:
        return [col for col in df.columns if col.endswith("_TAG")]

    @staticmethod
    def truncate_description(text):
        open_par = False
        for i, char in enumerate(text):
            if char == "(":
                open_par = True
            elif char == "[":
                if open_par == True:
                    return text[: i - 1], text[i:]
            else:
                open_par = False
        return text, np.NaN

    @staticmethod
    def get_type_date_from_path(path: str) -> Tuple[str, str, str]:
        """Extract the type of data and the date it was created from a file path

        Args:
            path (str): global path

        Returns:
            Tuple[str, str, str]: type, date, commit
        """
        path = Path(path)
        file_name = path.stem
        file_name_components = file_name.split("_")
        return (
            file_name_components[-3],
            file_name_components[-2],
            file_name_components[-1],
        )


if __name__ == "__main__":
    helm = HelmModels()
    helm.get_intermediate_from_raw()
