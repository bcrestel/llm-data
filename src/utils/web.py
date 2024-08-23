from typing import Any, Iterable

import requests
from bs4 import BeautifulSoup


def get_html_content_from_url(url: str) -> str:
    """
    Scrape a webpage and return the html in Unicode

    Args:
        url (str): url of the website to scrape

    Returns:
        str: html of the webpage in Unicode
    """
    response = requests.get(url)
    html_content = response.text
    return html_content


def find_section_from_html(html_content: str, name: str, class_: str) -> Iterable[Any]:
    """
    Find specific sections inside a html page in Unicode format

    Args:
        html_content (str): html of a webpage, in Unicode; likely coming from get_html_content_from_url
        name (str): name of section to look up
        class_ (str): class_ property of the section

    Returns:
        Iterable[Any]: list of html for each sections identified
    """
    soup = BeautifulSoup(html_content, "html.parser")
    raw_sections = soup.find_all(name, class_=class_)
    return raw_sections
