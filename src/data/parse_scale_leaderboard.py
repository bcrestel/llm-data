import requests
from bs4 import BeautifulSoup
from io import StringIO
import pandas as pd

from src.utils.constant import SCALE_LEADERBOARD_URL, SCALE_ADV_ROB

class ScaleLeaderbord:
    def __init__(self, url: str = SCALE_LEADERBOARD_URL) -> None:
        self.url = url
    
    def scrape(self) -> None:
        # scrape webpage
        response = requests.get(self.url)
        html_content = response.text
        soup = BeautifulSoup(html_content, 'html.parser')
        # find all relevant leaderboard tables
        tables = soup.find_all('div', class_="flex flex-col gap-4")
        self.tables = {}
        # parse all tables
        for table in tables:
            table_html = str(table.find('table'))
            table_io = StringIO(table_html)
            table_pd = pd.read_html(table_io)[0]

            table_name = table.find('span').text
            table_pd["leaderboard"] = table_name
            self.tables[table_name] = table_pd
        self.leaderbord_adv_rob = self.tables.pop(SCALE_ADV_ROB)
        self.leaderbord_metrics = pd.concat(list(self.tables.values), ignore_index=True)
    
    #def export_table(self) -> None:
    # TODO
