import requests
from bs4 import BeautifulSoup
from time import sleep

base_url = 'https://en.wikiquote.org/wiki/List_of_television_shows_'

pages = ['(A%E2%80%93H)', '(I%E2%80%93P)', '(Q%E2%80%93Z)']


def get_initial_tv_title_list():
    print("Starting Scraper for Wikiquote")

    titles = list()

    for page in pages:
        url = f'{base_url}{page}'
        print(f'Starting Page: {url}')

        skip_to_next_page = False

        result = requests.get(url)
        assert result.status_code == 200

        soup = BeautifulSoup(result.text, 'html.parser')

        ul_elements = soup.find_all('ul')
        assert ul_elements is not None

        for ul in ul_elements:

            if skip_to_next_page:
                break

            li_elements = ul.find_all('li')
            assert li_elements is not None

            for li in li_elements:
                a_tag = li.find('a')
                assert a_tag is not None

                if a_tag:
                    a_tag_text = a_tag.get_text()

                    if a_tag_text == 'Advertising slogans':
                        skip_to_next_page = True
                        break

                    titles.append(a_tag.get_text())

        print(f'Page done: {url}')
        sleep(3)

    print("Done with Wikiquote Scraper")
    return titles
