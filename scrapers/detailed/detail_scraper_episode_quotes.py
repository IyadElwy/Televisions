import requests
from bs4 import BeautifulSoup
from time import sleep


base_url = 'https://en.wikiquote.org/wiki/'


def check_for_nested_seasons(title_url_encoded):
    print(f'Starting check for seasons for {title_url_encoded}')

    urls = []

    initial = f'{base_url}{title_url_encoded}'
    response = requests.get(initial)
    assert response.status_code == 200

    season = 1
    while True:
        season_url = f'{base_url}{title_url_encoded}_(season_{season})'
        response = requests.get(season_url)

        if response.status_code == 404 and season == 1:
            print('No seasons found')
            return [title_url_encoded]

        elif response.status_code == 404:
            print(f'Found seasons: {urls}')
            return urls

        elif response.status_code == 200:
            urls.append(f'{title_url_encoded}_(season_{season})')
            season += 1

        sleep(2)


def scrape_page_for_quotes(soup):
    quotes = []

    dd_elements = soup.find_all("dd")
    assert dd_elements is not None

    for dd in dd_elements:
        quotes.append(dd.get_text())

    return quotes


def get_quotes_from_show(title_url_encoded):
    """
    title can come from wikiquotes url or wikipedia url (same format for titles in url)
    """
    print(f'Starting page: {title_url_encoded}')

    tv_quotes = {'urls': [],
                 'quotes': []}

    for current_url in check_for_nested_seasons(title_url_encoded):
        print(f'Starting sub-page: {current_url}')
        response = requests.get(f'{base_url}{current_url}')
        assert response.status_code == 200

        soup = BeautifulSoup(response.text, 'html.parser')
        quotes = scrape_page_for_quotes(soup)

        tv_quotes['urls'].append(f'{base_url}{current_url}')
        tv_quotes['quotes'].extend(quotes)

        print(f'Sub-page done')
        sleep(3)

    print(f'Page done')
    return tv_quotes


def get_detailed_info_about_all():
    pass
