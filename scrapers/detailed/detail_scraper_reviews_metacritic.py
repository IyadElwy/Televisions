import json
import requests
from time import sleep
from bs4 import BeautifulSoup
import aiohttp
import asyncio
from aiohttp.client_exceptions import ClientConnectionError, \
    ClientResponseError, ClientError, TooManyRedirects, InvalidURL


from utils.merging_with_cosmosDB import update_item_with_attribute

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

####################################################################################
# common to all functions


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


####################################################################################
# synchronous functions

def check_and_get_pages(initial, type):
    print(f"Checking for multiple pages for {type} review page")
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


####################################################################################
# async functions

async def async_check_and_get_pages(initial, type):
    print(f"Checking for multiple pages for {type} review page for {initial}")
    urls = []

    async with aiohttp.ClientSession() as session:
        async with session.get(initial, headers=headers) as response:
            response.raise_for_status()

    page = 0
    while True:
        print(f'Checking page: {page} for {initial}')
        page_url = f'{initial}?page={page}'
        async with aiohttp.ClientSession() as session:
            async with session.get(page_url, headers=headers) as response:
                response.raise_for_status()
                response_text = await response.text()

        soup = BeautifulSoup(response_text, 'html.parser')

        if not does_page_exists(soup, type=type):
            print(
                f'Page {page} not found for {initial}. Done looking for pages.')
            return urls

        urls.append(page_url)
        page += 1


async def async_extract_reviews_for_critic_page(initial_url):
    reviews = []

    print(f'Scraping critic review-page: {initial_url}')

    async with aiohttp.ClientSession() as session:
        async with session.get(initial_url, headers=headers) as response:
            response.raise_for_status()
            response_text = await response.text()

    soup = BeautifulSoup(response_text, 'html.parser')

    div_elements = soup.find_all('div', class_='review')
    assert div_elements is not None

    for div in div_elements:
        a_tags = div.find_all('a', class_='no_hover')
        assert a_tags is not None

        for a_tag in a_tags:
            score = div.select(
                'div.metascore_w')
            assert score is not None

            score = score[0].get_text() if len(score) > 0 else "50"
            reviews.append(
                {'review': a_tag.get_text().strip(), 'score': score})

    print(f'Done with critic review-page for {initial_url}')

    return reviews


async def async_extract_reviews_for_user_page(initial_url):
    reviews = []

    urls = await async_check_and_get_pages(initial_url, type='user')

    for url in urls:
        print(f'Scraping user review-page: {url}')

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                response_text = await response.text()

        soup = BeautifulSoup(response_text, 'html.parser')

        div_elements = soup.select('div.review')
        assert div_elements is not None

        for div in div_elements:
            spans = div.find_all('span')
            assert spans is not None

            score = div.find_all('div', class_='metascore_w')
            assert score is not None

            score = score[0].get_text() if len(score) > 0 else "50"
            reviews.append({'review': [s.get_text().strip()
                           for s in spans][2].strip(), 'score': score})

        print(f'Done with user review-page for {url}')

    return reviews


async def async_get_reviews_for_show(url, id):
    try:
        uniform_url = reformat_url_to_be_uniform(url)

        critic_review_url = f'{uniform_url}/critic-reviews'
        extracted_critic_reviews = await async_extract_reviews_for_critic_page(
            critic_review_url)

        user_review_url = f'{uniform_url}/user-reviews'
        extracted_user_reviews = await async_extract_reviews_for_user_page(user_review_url)

        result = {'critics': extracted_critic_reviews,
                  'users': extracted_user_reviews}

        update_item_with_attribute(id, 'metacritic', result)
        print(
            f'Updated data on CosmosDB with wikipedia data show with id: {id}')

    except (ClientError, ClientConnectionError,
            ClientResponseError, ClientError,
            TooManyRedirects, InvalidURL) as e:
        print({'Error': url,
               'status': e.status,
               'message': e.message,
               })


async def async_get_reviews_for_all():
    tasks = []

    with open('temp/merged_temp_tv_maze_data.ndjson', 'r') as file:
        for line in file:
            parsed_info = json.dumps(line)
            if 'metacritic_url' not in parsed_info or not parsed_info['metacritic_url']:
                continue

        tasks.append(async_get_reviews_for_show(
            parsed_info['metacritic_url'], parsed_info['id']))

    return await asyncio.gather(*tasks)

####################################################################################


def start_scraper():
    asyncio.run(async_get_reviews_for_all())
