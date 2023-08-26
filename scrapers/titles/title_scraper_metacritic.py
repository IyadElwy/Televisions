import requests
from bs4 import BeautifulSoup
import aiohttp
import asyncio
from services.aws_s3 import convert_to_json_and_save_to_s3


base_url = 'https://www.metacritic.com/browse/tv/title/all/{}?view=condensed&page={}'
headers = {
    "authority": "www.metacritic.com",
    "scheme": "https",
    "Accept": "text/html,application/xhtml+xml,application/xml",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.5",
    "Cache-Control": "max-age=0",
    "Sec-Ch-Ua": """Not/A)Brand";v="99", "Brave";v="115", "Chromium";v="115""",
    "Sec-Ch-Ua-Platform": "Linux",
    "Sec-Fetch-Dest": "ocument",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Sec-Gpc": "1",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
}

alphabet = [chr(ord('a') + i) for i in range(26)]
alphabet.insert(0, '')

####################################################################################
# common to all functions


def does_page_exists(soup_object):
    no_data = soup_object.find("p", class_='no_data')

    if no_data and no_data.get_text() == 'No Results Found':
        return False

    return True


####################################################################################
# synchronous functions


def get_initial_tv_title_list():
    print("Starting Scraper for Metacritic")

    shows = list()

    for letter in alphabet:
        page = 0

        while True:

            url = base_url.format(letter, page)
            print(f'Starting Page: {url}')

            response = requests.get(url, headers=headers)
            assert response.status_code == 200

            soup = BeautifulSoup(response.text, 'html.parser')

            if not does_page_exists(soup_object=soup):
                break

            ol_elements = soup.find('ol', class_='list_product_condensed')
            assert ol_elements is not None

            div_elements = ol_elements.find_all(  # type: ignore
                'div', class_='product_title')
            assert div_elements is not None

            for div in div_elements:
                a_tag = div.find('a')

                if a_tag:
                    shows.append(
                        (a_tag.get_text().strip(), f'https://www.metacritic.com{a_tag.get("href")}'))

            print(f'Page done')

            page += 1

    print("Done with Metacritic Scraper")

    return shows


####################################################################################
# async functions


async def async_get_initial_tv_title_list():
    print("Starting Scraper for Metacritic")

    shows = list()

    for letter in alphabet:
        page = 0

        while True:

            url = base_url.format(letter, page)
            print(f'Starting Page: {url}')

            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    response.raise_for_status()
                    response_text = await response.text()

            soup = BeautifulSoup(response_text, 'html.parser')

            if not does_page_exists(soup_object=soup):
                break

            ol_elements = soup.find('ol', class_='list_product_condensed')
            assert ol_elements is not None

            div_elements = ol_elements.find_all(  # type: ignore
                'div', class_='product_title')
            assert div_elements is not None

            for div in div_elements:
                a_tag = div.find('a')

                if a_tag:
                    shows.append(
                        (a_tag.get_text().strip(), f'https://www.metacritic.com{a_tag.get("href")}'))

            print(f'Page done')

            page += 1

    print("Done with Metacritic Scraper")

    try:
        convert_to_json_and_save_to_s3(shows, 'televisions-raw-titles-urls',
                                       'metacritic.json')
    except Exception as e:
        print("Error loading Metacritic titles to s3")

####################################################################################


def start_scraper():
    asyncio.run(async_get_initial_tv_title_list())
