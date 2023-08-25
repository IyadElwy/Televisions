import requests
from bs4 import BeautifulSoup
import sys

from services.aws_s3 import save_titles_url_to_s3

url = 'https://eztv.re/showlist/'

####################################################################################


def get_initial_tv_title_list():
    print("Starting Scraper for eztv")

    shows = list()

    response = requests.get(url)
    assert response.status_code == 200

    soup = BeautifulSoup(response.text, 'html.parser')

    tr_elements = soup.find_all('tr')
    assert tr_elements is not None

    for tr in tr_elements:
        td_title_a_tag_element = tr.find_all('td')[0]
        assert td_title_a_tag_element is not None

        a_tag = td_title_a_tag_element.find("a")

        if a_tag:

            shows.append(
                (a_tag.get_text(), f'https://eztv.re/{a_tag.get("href")}'))

    print("Done with eztv Scraper")

    shows = shows[2:]
    try:
        save_titles_url_to_s3(shows, 'eztv.json')
    except Exception as e:
        print("Error loading eztv titles to s3")

####################################################################################


def start_scraper():
    get_initial_tv_title_list()
