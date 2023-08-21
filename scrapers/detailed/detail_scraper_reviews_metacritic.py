import json
import requests
from time import sleep
from bs4 import BeautifulSoup


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


def reformat_url_to_be_uniform(url):
    return url.split('/season-')[0]


def does_page_exists(soup_object, type):
    if type == 'user':
        no_data = soup_object.find('div', string="No reviews yet.")
        if no_data:
            return False
        return True
    else:
        return False


def check_and_get_pages(initial, type):
    print("Checking for multiple pages")
    urls = []

    response = requests.get(initial, headers=headers)
    assert response.status_code == 200

    page = 0
    while True:
        print(f'Checking page: {page}')
        page_url = f'{initial}?page={page}'
        response = requests.get(page_url,  headers=headers)
        assert response.status_code == 200

        soup = BeautifulSoup(response.text, 'html.parser')

        if not does_page_exists(soup, type=type):
            print('Done looking for pages')
            return urls

        urls.append(page_url)
        page += 1

        sleep(1)


def extract_reviews_for_critic_page(initial_url):
    reviews = []

    print(f'Scraping critic review-page: {initial_url}')

    response = requests.get(initial_url, headers=headers)
    assert response.status_code == 200

    soup = BeautifulSoup(response.text, 'html.parser')

    div_elements = soup.find_all('div', class_='review')
    assert div_elements is not None

    for div in div_elements:
        a_tags = div.find_all('a', class_='no_hover')
        assert a_tags is not None

        for a_tag in a_tags:
            reviews.append(a_tag.get_text().strip())

    print(f'Done with critic review-page')

    return reviews


def extract_reviews_for_user_page(initial_url):
    reviews = []

    urls = check_and_get_pages(initial_url, type='user')

    for url in urls:
        print(f'Scraping user review-page: {url}')

        response = requests.get(url, headers=headers)
        assert response.status_code == 200

        soup = BeautifulSoup(response.text, 'html.parser')

        div_elements = soup.find_all('div', class_='review_body')
        assert div_elements is not None

        for div in div_elements:
            span = div.find('span')
            assert span is not None

            reviews.append(span.get_text())

        print(f'Done with user review-page')

    return reviews


def get_reviews_for_show(url):
    uniform_url = reformat_url_to_be_uniform(url)

    critic_review_url = f'{uniform_url}/critic-reviews'
    extracted_critic_reviews = extract_reviews_for_critic_page(
        critic_review_url)

    user_review_url = f'{uniform_url}/user-reviews'
    extracted_user_reviews = extract_reviews_for_user_page(user_review_url)

    sleep(3)

    return {'critics': extracted_critic_reviews,
            'users': extracted_user_reviews}


def get_detailed_info_about_all():
    pass


print(json.dumps(get_reviews_for_show(
    'https://www.metacritic.com/movie/meg-2-the-trench')))
