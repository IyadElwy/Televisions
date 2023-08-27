import requests
from bs4 import BeautifulSoup
from time import sleep
import aiohttp
import asyncio
from aiohttp.client_exceptions import ClientConnectionError, \
    ClientResponseError, ClientError, TooManyRedirects, InvalidURL
import json

base_url = 'https://en.wikiquote.org/wiki/'

####################################################################################
# common to all functions


def scrape_page_for_quotes(soup):
    quotes = []

    dd_elements = soup.find_all("dd")
    assert dd_elements is not None

    for dd in dd_elements:
        quotes.append(dd.get_text())

    return quotes


####################################################################################
# synchronous functions


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

####################################################################################
# async functions


async def async_check_for_nested_seasons(title_url_encoded):
    print(f'Starting check for seasons for {title_url_encoded}')

    urls = []

    initial = f'{base_url}{title_url_encoded}'
    async with aiohttp.ClientSession() as session:
        async with session.get(initial) as response:
            response.raise_for_status()

    season = 1
    while True:
        season_url = f'{base_url}{title_url_encoded}_(season_{season})'
        async with aiohttp.ClientSession() as session:
            async with session.get(season_url) as response:

                if response.status == 404 and season == 1:
                    print(f'No seasons found for {title_url_encoded}')
                    return [title_url_encoded]

                elif response.status == 404:
                    print(f'Found seasons: {urls}')
                    return urls

                elif response.status == 200:
                    urls.append(f'{title_url_encoded}_(season_{season})')
                    season += 1


async def async_get_quotes_from_show(title_url_encoded):
    """
    title can come from wikiquotes url or wikipedia url (same format for titles in url)
    """
    print(f'Starting page: {title_url_encoded}')

    tv_quotes = []

    try:

        for current_url in await async_check_for_nested_seasons(title_url_encoded):
            print(f'Starting sub-page: {current_url}')

            async with aiohttp.ClientSession() as session:
                async with session.get(f'{base_url}{current_url}') as response:
                    response.raise_for_status()
                    response_text = await response.text()

            soup = BeautifulSoup(response_text, 'html.parser')
            quotes = scrape_page_for_quotes(soup)

            tv_quotes.extend(quotes)

            print(f'Sub-page done for {current_url}')

        print(f'Page done for {title_url_encoded}')

        # save tv_quotes to CosmoDB

    except (ClientError, ClientConnectionError,
            ClientResponseError, ClientError,
            TooManyRedirects, InvalidURL) as e:
        print({'Error': title_url_encoded,
               'status': e.status,
               'message': e.message,
               })


async def async_get_quotes_for_all():
    tasks = []

    # get wikipedia/wikiquotes urls from CosmoDB and loop over them
    # split to get the encoded titles at the end
    encoded_titles = 1 * ['ICarly']

    for encoded_title in encoded_titles:
        tasks.append(async_get_quotes_from_show(encoded_title))

    return await asyncio.gather(*tasks)

####################################################################################


def start_scraper():
    asyncio.run(async_get_quotes_for_all())
