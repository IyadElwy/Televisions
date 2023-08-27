import json
import requests
import aiohttp
import asyncio
from aiohttp.client_exceptions import ClientConnectionError, ClientResponseError
from time import sleep
from services.aws_rds import retrieve_data

detailed_single_search_base_url = 'https://api.tvmaze.com/singlesearch/shows?q={}{}'

detailed_retrieve_by_id_base_url = 'https://api.tvmaze.com/shows/{}{}'

season_detailed_retrieve_by_id_base_url = 'https://api.tvmaze.com/shows/{}/seasons'

####################################################################################
# synchronous functions


def detailed_single_search(query, **kwargs):
    """
    embed[]=episodes&embed[]=cast
    """

    if kwargs:
        url = detailed_single_search_base_url.format(
            query,
            ''.join(['&embed[]=' + key for key in kwargs if kwargs[key]]))
        response = requests.get(url)
        assert response.status_code == 200

        return response.json()

    else:
        url = detailed_single_search_base_url.format(query, '')
        response = requests.get(url)
        assert response.status_code == 200

        return response.json()


def detailed_retrieve_by_id(id, **kwargs):
    """
    embed[]=episodes&embed[]=cast
    """

    if kwargs:
        url = detailed_retrieve_by_id_base_url.format(
            id,
            ''.join(['&embed[]=' + key for key in kwargs if kwargs[key]]))
        response = requests.get(url)
        assert response.status_code == 200

        return response.json()

    else:
        url = detailed_retrieve_by_id_base_url.format(id, '')
        response = requests.get(url)
        assert response.status_code == 200

        return response.json()


def season_detailed_retrieve_by_id(id):
    url = season_detailed_retrieve_by_id_base_url.format(id)
    response = requests.get(url)
    assert response.status_code == 200

    return response.json()


def get_detailed_info_about_all():
    pass

####################################################################################
# async functions


async def async_detailed_single_search(query, **kwargs):
    """
    embed[]=episodes&embed[]=cast
    """

    if kwargs:
        url = detailed_single_search_base_url.format(
            query,
            ''.join(['&embed[]=' + key for key in kwargs if kwargs[key]]))

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()
                return await response.json()

    else:
        url = detailed_single_search_base_url.format(query, '')

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()
                return await response.json()


async def async_detailed_retrieve_by_id(id, **kwargs):
    """
    embed[]=episodes&embed[]=cast
    """

    if kwargs:
        url = detailed_retrieve_by_id_base_url.format(
            id,
            ''.join(['&embed[]=' + key for key in kwargs if kwargs[key]]))
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()
                return await response.json()

    else:
        url = detailed_retrieve_by_id_base_url.format(id, '')
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()
                return await response.json()


async def async_season_detailed_retrieve_by_id(id):
    url = season_detailed_retrieve_by_id_base_url.format(id)
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            response.raise_for_status()
            return await response.json()


async def async_get_and_merge_info(title, record_to_merge):
    print(f'Starting detail extraction for {title}')
    try:
        single_search_result = await async_detailed_single_search(title, cast=True, episodes=True)
        season_search_result = await async_season_detailed_retrieve_by_id(single_search_result['id'])
        single_search_result['seasons'] = season_search_result
        data = single_search_result
        data['wikipedia_url'] = record_to_merge[2]
        data['wikiquote_url'] = record_to_merge[3]
        data['metacritic_url'] = record_to_merge[4]
        data['eztv_url'] = record_to_merge[5]
        # save_to_typesense(data)
        print(f'Extraction done for {title}')
        return data

    except ClientResponseError as not_found:

        print({'not_found': title,
               'status': not_found.status,
               'message': not_found.message,
               })

    except ClientConnectionError as e:
        print("Limit reached, sleeping for 10 seconds...", title)
        await asyncio.sleep(10)
        return await async_get_and_merge_info(title, record_to_merge)


async def process_title(title, record_to_merge):
    return await async_get_and_merge_info(title, record_to_merge)


async def async_get_detailed_info_about_all():
    tasks = []

    chunk_size = 100
    offset = 0

    print('Retrieving chunks from Postgres RDS')
    while True:
        records = retrieve_data(
            f'SELECT * FROM tv_shows LIMIT {chunk_size} OFFSET {offset}')

        if not records:
            break

        tasks.extend([process_title(record[-1], record) for record in records])

        print(f'Got chunk starting with offset {offset}')
        offset += chunk_size

    return await asyncio.gather(*tasks)

####################################################################################


def start_scraper():
    print("Starting detailed scraper for tv maze")
    asyncio.run(async_get_detailed_info_about_all())
    print("Done with detailed scraper for tv maze")
