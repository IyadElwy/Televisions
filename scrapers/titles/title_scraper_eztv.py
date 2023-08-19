import requests
from bs4 import BeautifulSoup

url = 'https://eztv.re/showlist/'


def get_initial_tv_titles_list():
    print("Starting Scraper for eztv")

    titles = list()

    result = requests.get(url)
    assert result.status_code == 200

    soup = BeautifulSoup(result.text, 'html.parser')

    tr_elements = soup.find_all('tr')
    assert tr_elements is not None

    for tr in tr_elements:
        td_title_a_tag_element = tr.find_all('td')[0]
        assert td_title_a_tag_element is not None

        a_tag = td_title_a_tag_element.find("a")
        assert a_tag is not None

        if a_tag:
            titles.append(a_tag.get_text())

    print("Done with eztv Scraper")

    return titles[2:]
