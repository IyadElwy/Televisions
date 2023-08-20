import requests
from bs4 import BeautifulSoup
from time import sleep
import json
import re


base_url = 'https://en.wikiquote.org/wiki/'


def check_for_nested_seasons(url):
    urls = []
    # check if (season_1) gets anything and if yes paginate until we have the urls of all pages that have something
    # else return the url
    pass


def scrape_page_for_quotes(soup):

    quotes = []

    dd_elements = soup.find_all("dd")
    for dd in dd_elements:
        quotes.append(dd.get_text())

    return quotes


def get_quotes_from_show(url):
    tv_quotes = {'url': '',
                 'quotes': []}

    url_element = f'{base_url}{url}'
    tv_quotes['url'] = url_element

    print(f'Starting Page: {url_element}')

    for current_url in check_for_nested_seasons(url):

        response = requests.get(current_url)
        assert response.status_code == 200

        soup = BeautifulSoup(response.text, 'html.parser')
        quotes = scrape_page_for_quotes(soup)
        tv_quotes['quotes'].extend(quotes)

        print(f'Page done')
        sleep(3)


print(json.dumps(get_quotes_from_show('ICarly')))


def get_detailed_info_about_all():
    pass
