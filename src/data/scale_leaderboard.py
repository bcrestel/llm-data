import logging
import pickle
import re
from datetime import datetime
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

from src.utils.constant import (
    LOCAL_PATH_TO_INT_DATA,
    LOCAL_PATH_TO_RAW_DATA,
    SCALE_COL_MODEL,
    SCALE_EVAL_ADV_ROB,
    SCALE_EVAL_MAPPING,
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
        file_name = Path(LOCAL_PATH_TO_RAW_DATA) / self.file_name_raw()
        with open(file_name, "wb") as file:
            pickle.dump(html_content, file)

    @staticmethod
    def file_name_raw() -> Path:
        now = datetime.now()
        date = now.strftime("%Y-%m-%d")
        return Path(f"scale_leaderboard_{date}_raw.pickle")

    def parse_html(self) -> None:
        # Load html
        file_name = Path(LOCAL_PATH_TO_RAW_DATA) / self.file_name_raw()
        with open(file_name, "rb") as file:
            html_content = pickle.load(file)
        logger.info(f"Loaded file {file_name}")
        # Extract tables
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
        # TODO: Add a timestamp, based on raw file name
        logger.debug(joined_table)
        # TODO: Save table to intermediate folder

    @staticmethod
    def remove_leading_number(text):
        return re.sub(r"^\d+(?:st|nd|rd)?", "", text)


if __name__ == "__main__":
    sc = ScaleLeaderbord()
    # sc.get_raw_data()
    sc.parse_html()
