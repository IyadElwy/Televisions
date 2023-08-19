import requests
from bs4 import BeautifulSoup
from time import sleep


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


def does_page_exists(soup_object):
    no_data = soup_object.find("p", class_='no_data')

    if no_data and no_data.get_text() == 'No Results Found':
        return False

    return True


def get_initial_tv_title_list():
    print("Starting Scraper for Metacritic")

    titles = list()

    for letter in alphabet:
        page = 0

        while True:

            url = base_url.format(letter, page)
            print(f'Starting Page: {url}')

            result = requests.get(url, headers=headers)
            assert result.status_code == 200

            soup = BeautifulSoup(result.text, 'html.parser')

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
                    titles.append(a_tag.get_text().strip())

            print(f'Page done: {url}')
            sleep(3)

            page += 1

    print("Done with Metacritic Scraper")

    return titles


get_initial_tv_title_list()
