import requests
from bs4 import BeautifulSoup
from time import sleep

base_url = 'https://en.wikipedia.org/wiki/List_of_television_programs:'
pages = ['_numbers',
         '_A',
         '_B',
         '_C',
         '_D',
         '_E',
         '_F',
         '_G',
         '_H',
         '_I%E2%80%93J',
         '_K%E2%80%93L',
         '_M',
         '_N',
         '_O',
         '_P',
         '_Q%E2%80%93R',
         '_S',
         '_T',
         '_U%E2%80%93V%E2%80%93W',
         '_X%E2%80%93Y%E2%80%93Z',]


def get_initial_tv_title_list():
    print("Starting Scraper for Wikipedia")

    shows = list()

    for page in pages:
        url = f'{base_url}{page}'
        print(f'Starting Page: {url}')

        response = requests.get(url)
        assert response.status_code == 200

        soup = BeautifulSoup(response.text, 'html.parser')

        main_div = soup.select_one('#mw-content-text > div.mw-parser-output')
        assert main_div is not None

        ul_elements = main_div.select('ul')
        assert ul_elements is not None

        for ul in ul_elements:
            a_tags = ul.find_all('a')
            assert a_tags is not None

            for a_tag in a_tags:
                if a_tag:
                    shows.append(
                        (a_tag.get_text(),  f'https://en.wikipedia.org{a_tag.get("href")}'))

        print(f'Page done')
        sleep(3)

    print("Done with Wikipedia Scraper")
    return shows[3:]
