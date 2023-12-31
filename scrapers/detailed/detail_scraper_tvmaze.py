import os
import json
import requests
import aiohttp
import asyncio
from aiohttp.client_exceptions import ClientConnectionError, ClientResponseError
from time import sleep
from services.aws_rds import retrieve_data
from services.azure_cosmos_db import CosmosDbConnection
from pathlib import Path
from azure.cosmos.exceptions import CosmosResourceExistsError, CosmosHttpResponseError, CosmosResourceNotFoundError

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


def save_json_to_local_temp_file(file, data):
    file.write(json.dumps(data) + '\n')


def merge_data():
    print('Starting merging of data')
    merged_data_items = {}

    with open(f'temp/temp_tv_maze_data.ndjson', 'r') as file:
        for line in file:
            data = json.loads(line)
            if data['id'] not in merged_data_items:
                merged_data_items[data['id']] = data
            else:
                existing_data = merged_data_items[data['id']]
                urls = {
                    'wikipedia_url':  existing_data["wikipedia_url"],
                    'wikiquote_url': existing_data["wikiquote_url"],
                    'metacritic_url': existing_data["metacritic_url"],
                    'eztv_url': existing_data["eztv_url"],
                }

                for url in urls.keys():
                    if not urls[url] and data[url]:
                        merged_data_items[data['id']][url] = data[url]

    with open(f'temp/merged_temp_tv_maze_data.ndjson', 'a') as file:
        for item in merged_data_items.values():
            file.write(json.dumps(item) + '\n')

    print('Done with merging of data')


def update_or_create_item_with_attribute(container, id, attribute_name, attribute_body):
    try:
        show = list(container.query_items(
            query=f'SELECT * FROM c WHERE c.id="{id}"',
            enable_cross_partition_query=True))
        if len(show) == 0:
            raise CosmosResourceNotFoundError()

        show = show[0]
        show[attribute_name] = attribute_body
        container.replace_item(item=id, body=show)

    except CosmosResourceNotFoundError as e:
        container.create_item(body={attribute_name: attribute_body})
    except CosmosHttpResponseError as e:
        if e.status_code == 429:
            sleep_amount = (e.headers['x-ms-retry-after-ms'] / 1000) + 1
            print(
                f'Encountered: {e.message}. Sleeping for {sleep_amount} seconds')
            sleep(sleep_amount)
            update_or_create_item_with_attribute(
                container, id, attribute_name, attribute_body)
        else:
            print(f'Error during updating of document with id: {id}. {e}')


def save_to_cosmosDB(container, data):
    update_or_create_item_with_attribute(
        container, data['id'], 'id', data['id'])
    for key in data.keys():
        update_or_create_item_with_attribute(
            container, data['id'],  key, data[key])
    print(f'Saved {data["name"]}')


def read_merged_data_and_save_to_cosmoDB():
    print('Starting saving of data')

    with open('temp/merged_temp_tv_maze_data.ndjson', 'r') as file:
        with CosmosDbConnection() as connection:
            container = connection.container
            for line in file:
                data = json.loads(line)
                data['id'] = str(data['id'])
                save_to_cosmosDB(container, data)

    # file_path_data = Path('temp/temp_tv_maze_data.ndjson')
    # file_path_merged_data = Path('temp/merged_temp_tv_maze_data.ndjson')

    # if file_path_data.exists():
    #     file_path_data.unlink()
    # if file_path_merged_data.exists():
    #     file_path_merged_data.unlink()
    print('Done saving data')


async def async_get_and_merge_info(title, record_to_merge, file):
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
        save_json_to_local_temp_file(file, data)
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
        return await async_get_and_merge_info(title, record_to_merge, file)


async def async_get_detailed_info_about_all(file):
    tasks = []

    chunk_size = 100
    offset = 0

    print('Retrieving chunks from Postgres RDS')
    while True:
        records = retrieve_data(
            f'SELECT * FROM tv_shows LIMIT {chunk_size} OFFSET {offset}')

        if not records:
            break

        tasks.extend([async_get_and_merge_info(record[-1], record, file)
                     for record in records])

        await asyncio.gather(*tasks)
        tasks = []

        print(f'Got chunk starting with offset {offset}')
        offset += chunk_size

    return

####################################################################################


def start_scraper():
    if not os.path.exists('temp'):
        os.makedirs('temp')
    file = open('temp/temp_tv_maze_data.ndjson', mode='a')
    print("Starting detailed scraper for tv maze")
    asyncio.run(async_get_detailed_info_about_all(file))
    file.close()
    print("Done with detailed scraper for tv maze")
    print('Written data locally')
