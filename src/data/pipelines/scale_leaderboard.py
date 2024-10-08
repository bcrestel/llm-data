import glob
import logging
import re
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

from src.utils.constant import (
    LOCAL_PATH_TO_INT_DATA,
    LOCAL_PATH_TO_RAW_DATA,
    SCALE_COL_MODEL,
    SCALE_EVAL_ADV_ROB,
    SCALE_EVAL_MAPPING,
    SCALE_LEADERBOARD_FILE_PREFIX,
    SCALE_LEADERBOARD_URL,
)
from src.utils.date import get_date_YYYY_MM_DD
from src.utils.io.pickle import load_from_pickle, save_to_pickle
from src.utils.web import find_section_from_html, get_html_content_from_url

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
        html_content = get_html_content_from_url(self.url)
        # serialize to data/raw
        file_name = Path(LOCAL_PATH_TO_RAW_DATA) / self.file_name(
            type="raw", extension="pickle"
        )
        logger.info(f"Saving url {self.url} to file {file_name}")
        save_to_pickle(file_name=file_name, content=html_content)

    @staticmethod
    def file_name(type: str, extension: str, date: Optional[datetime] = None) -> Path:
        date = get_date_YYYY_MM_DD() if date is None else date
        return Path(f"{SCALE_LEADERBOARD_FILE_PREFIX}_{type}_{date}.{extension}")

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

    # TODO: rename: process_from_raw_to_intermediate
    def get_intermediate_from_raw(self, file_name: Optional[Path] = None) -> None:
        if file_name is None:
            # if no file_name provided, select the most recent raw file
            file_pattern = (
                Path(LOCAL_PATH_TO_RAW_DATA)
                / f"{SCALE_LEADERBOARD_FILE_PREFIX}_*.pickle"
            )
            file_names = {}
            for ff in glob.glob(str(file_pattern)):
                _, date = self.get_type_date_from_path(ff)
                file_names[pd.to_datetime(date)] = ff
            date = min(file_names.keys())
            file_name = file_names[date]
        # Load html in pickle format
        html_content = load_from_pickle(file_name=file_name)
        logger.info(f"Loaded file {file_name}")
        # Extract tables from the html
        raw_tables = find_section_from_html(
            html_content, name="div", class_="flex flex-col gap-4"
        )
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
                table_pd.rename(columns={"Number of Violations": "score"}, inplace=True)
                table_pd["score_type"] = "Number of violations"
            else:
                table_pd.rename(columns={"Score": "score"}, inplace=True)
                table_pd["score_type"] = "Score"
            # Split 95% CI column
            table_pd[["95CI_max", f"95CI_min"]] = (
                table_pd["95% Confidence"]
                .str.split("/", expand=True)
                .astype(float)
                .apply(lambda x: x + table_pd["score"])
            )
            table_pd.drop(columns="95% Confidence", axis=1, inplace=True)
            table_pd = table_pd[
                [
                    SCALE_COL_MODEL,
                    "95CI_min",
                    "score",
                    "95CI_max",
                ]
            ]
            table_pd["evaluation_type"] = table_name
            logger.debug(table_pd)
            tables.append(table_pd)
        # Concatenate all tables
        joined_table = pd.concat(tables, axis=0, ignore_index=True)
        # Add a timestamp
        _, date = self.get_type_date_from_path(file_name)
        joined_table["date_evaluation"] = pd.to_datetime(date)
        # Add source
        joined_table["raw_source"] = file_name

        logger.debug(joined_table)
        # Save joined table in 02_intermediate folder
        output_file_path = Path(LOCAL_PATH_TO_INT_DATA) / self.file_name(
            type="intermediate", extension="parquet", date=date
        )
        joined_table.to_parquet(path=output_file_path)
        logger.info(f"Saved formatted Dataframe to {output_file_path}")

    @staticmethod
    def remove_leading_number(text):
        return re.sub(r"^\d+(?:st|nd|rd)?", "", text)

    def long_to_wide(self, long_df: pd.DataFrame) -> pd.DataFrame:
        # TODO: Implement
        #            # Rename score column
        #            if table_name == SCALE_EVAL_ADV_ROB:
        #                table_pd.rename(columns={"Number of Violations": "Score"}, inplace=True)
        #            score_new_name = f"Score_{SCALE_EVAL_MAPPING[table_name]}"
        #            table_pd.rename(columns={"Score": score_new_name}, inplace=True)
        #            # Split 95% CI column
        #            table_pd[[f"{score_new_name}_95CI_max", f"{score_new_name}_95CI_min"]] = (
        #                table_pd["95% Confidence"]
        #                .str.split("/", expand=True)
        #                .astype(float)
        #                .apply(lambda x: x + table_pd[score_new_name])
        #            )
        #            table_pd.drop(columns="95% Confidence", axis=1, inplace=True)
        #            table_pd = table_pd[
        #                [
        #                    SCALE_COL_MODEL,
        #                    f"{score_new_name}_95CI_min",
        #                    score_new_name,
        #                    f"{score_new_name}_95CI_max",
        #                ]
        #            ]
        raise NotImplemented


if __name__ == "__main__":
    sc = ScaleLeaderbord()
    # sc.get_raw_data()
    sc.get_intermediate_from_raw()
