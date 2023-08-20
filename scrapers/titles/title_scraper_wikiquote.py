import requests
from bs4 import BeautifulSoup
from time import sleep

base_url = 'https://en.wikiquote.org/wiki/List_of_television_shows_'

pages = ['(A%E2%80%93H)', '(I%E2%80%93P)', '(Q%E2%80%93Z)']


def get_initial_tv_title_list():
    print("Starting Scraper for Wikiquote")

    shows = list()

    for page in pages:
        url = f'{base_url}{page}'
        print(f'Starting Page: {url}')

        skip_to_next_page = False

        response = requests.get(url)
        assert response.status_code == 200

        soup = BeautifulSoup(response.text, 'html.parser')

        ul_elements = soup.find_all('ul')
        assert ul_elements is not None

        for ul in ul_elements:

            if skip_to_next_page:
                break

            li_elements = ul.find_all('li')
            assert li_elements is not None

            for li in li_elements:
                a_tag = li.find('a')

                if a_tag:
                    a_tag_text = a_tag.get_text()

                    if a_tag_text == 'Advertising slogans':
                        skip_to_next_page = True
                        break

                    shows.append(
                        (a_tag.get_text(), f'https://en.wikiquote.org{a_tag.get("href")}'))

        print(f'Page done')
        sleep(3)

    print("Done with Wikiquote Scraper")
    return shows
