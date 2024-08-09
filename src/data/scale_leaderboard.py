import glob
import logging
import pickle
import re
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup

from src.utils.constant import (
    LOCAL_PATH_TO_INT_DATA,
    LOCAL_PATH_TO_RAW_DATA,
    SCALE_COL_MODEL,
    SCALE_EVAL_ADV_ROB,
    SCALE_EVAL_MAPPING,
    SCALE_LEADERBOARD_FILE_PREFIX,
    SCALE_LEADERBOARD_URL,
)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(filename)s--l.%(lineno)d: %(message)s",
)
logger = logging.getLogger(__name__)


class ScaleLeaderbord:
    def __init__(self, url: str = SCALE_LEADERBOARD_URL) -> None:
        self.url = url

    def get_raw_data(self) -> None:
        """Scrape webpage and save html"""
        # scrape webpage
        response = requests.get(self.url)
        html_content = response.text
        # serialize to data/raw
        file_name = Path(LOCAL_PATH_TO_RAW_DATA) / self.file_name(type="raw")
        logger.info(f"Saving url {self.url} to file {file_name}")
        with open(file_name, "wb") as file:
            pickle.dump(html_content, file)

    @staticmethod
    def file_name(type: str, date: Optional[datetime] = None) -> Path:
        if date is None:
            now = datetime.now()
            date = now.strftime("%Y-%m-%d")
        return Path(f"{SCALE_LEADERBOARD_FILE_PREFIX}_{type}_{date}.pickle")

    @staticmethod
    def get_type_date_from_path(path: str) -> Tuple[str, str]:
        """Extract the type of data and the date it was created from a file path

        Args:
            path (str): global path

        Returns:
            Tuple[str, str]: type, date
        """
        path = Path(path)
        file_name = path.stem
        file_name_components = file_name.split("_")
        return file_name_components[-2], file_name_components[-1]

    def parse_html(self, file_name: Optional[Path] = None) -> None:
        if file_name is None:
            # if no file_name provided, select the most recent raw file
            file_pattern = (
                Path(LOCAL_PATH_TO_RAW_DATA)
                / f"{SCALE_LEADERBOARD_FILE_PREFIX}_*.pickle"
            )
            file_names = {}
            for ff in glob.glob(str(file_pattern)):
                type, date = self.get_type_date_from_path(ff)
                file_names[pd.to_datetime(date)] = ff
            date = min(file_names.keys())
            file_name = file_names[date]
        # Load html in pickle format
        with open(file_name, "rb") as file:
            html_content = pickle.load(file)
        logger.info(f"Loaded file {file_name}")
        # Extract tables from the html
        soup = BeautifulSoup(html_content, "html.parser")
        raw_tables = soup.find_all("div", class_="flex flex-col gap-4")
        tables = []
        # process all tables
        for table in raw_tables:
            # parse table
            table_html = str(table.find("table"))
            table_io = StringIO(table_html)
            table_pd = pd.read_html(table_io)[0]

            table_name = table.find("span").text
            logger.info(f"Processing table {table_name}")
            # process column Model
            table_pd[SCALE_COL_MODEL] = table_pd[SCALE_COL_MODEL].apply(
                self.remove_leading_number
            )
            # Rename score column
            if table_name == SCALE_EVAL_ADV_ROB:
                table_pd.rename(columns={"Number of Violations": "Score"}, inplace=True)
            score_new_name = f"Score_{SCALE_EVAL_MAPPING[table_name]}"
            table_pd.rename(columns={"Score": score_new_name}, inplace=True)
            # Split 95% CI column
            table_pd[[f"{score_new_name}_95CI_max", f"{score_new_name}_95CI_min"]] = (
                table_pd["95% Confidence"]
                .str.split("/", expand=True)
                .astype(float)
                .apply(lambda x: x + table_pd[score_new_name])
            )
            table_pd.drop(columns="95% Confidence", axis=1, inplace=True)
            table_pd = table_pd[
                [
                    SCALE_COL_MODEL,
                    f"{score_new_name}_95CI_min",
                    score_new_name,
                    f"{score_new_name}_95CI_max",
                ]
            ]
            logger.debug(table_pd)
            tables.append(table_pd)
        # Do an outer join of all tables
        joined_table = tables.pop()
        for table in tables:
            joined_table = pd.merge(
                joined_table, table, on=SCALE_COL_MODEL, how="outer"
            )
        # Add a timestamp
        type, date = self.get_type_date_from_path(file_name)
        joined_table["date"] = pd.to_datetime(date)
        # I don't think this is the date that should be used. This date should be the model release date. Once a model version is pinned, the date the evaluation is ran is irrelevant.
        # Pricing for a given model will actually change. But that's a different dataset
        # TODO: Build a directory of all models along with their information (release date, parameters,...)
        logger.debug(joined_table)
        # Save joined table in 02_intermediate folder
        output_file_path = Path(LOCAL_PATH_TO_INT_DATA) / self.file_name(
            type="intermediate"
        )
        joined_table.to_parquet(path=output_file_path)
        logger.info(f"Saved formatted Dataframe to {output_file_path}")

    @staticmethod
    def remove_leading_number(text):
        return re.sub(r"^\d+(?:st|nd|rd)?", "", text)


if __name__ == "__main__":
    sc = ScaleLeaderbord()
    # sc.get_raw_data()
    sc.parse_html()
