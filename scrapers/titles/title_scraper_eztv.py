import requests
from bs4 import BeautifulSoup

url = 'https://eztv.re/showlist/'


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

    return shows[2:]