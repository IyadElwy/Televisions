from services.azure_cosmos_db import CosmosDbConnection
import azure.cosmos.exceptions as exceptions
import json
from pathlib import Path


def save_json_to_local_temp_file(data, filename):
    with open(f'temp/{filename}', 'a+') as file:
        file.write(json.dumps(data) + '\n')


def delete_temp_file_needed_for_scraping():
    file_path = 'temp/data_needed_for_detailed_scraper.ndjson'
    file_path_data = Path(file_path)

    if file_path_data.exists():
        file_path_data.unlink()


def update_item_with_attribute(id, attribute_name, attribute_body):
    try:
        with CosmosDbConnection() as conn:
            show = list(conn.container.query_items(
                query=f'SELECT * FROM c WHERE c.id="{id}"',
                enable_cross_partition_query=True))[0]

            show[attribute_name] = attribute_body
            conn.container.replace_item(item=id, body=show)
    except exceptions.CosmosResourceNotFoundError as e:
        print(f'Not found with id: {id}')
    except exceptions.CosmosHttpResponseError as e:
        print(f'Error during updating of document with id: {id}')
    except Exception as e:
        print(f'Problem while updating: {e}')


def save_temp_data_needed_for_detail_scraping():
    chunk_size = 50
    offset = 0

    try:
        with CosmosDbConnection() as conn:
            while True:
                shows = list(conn.container.query_items(
                    query=f'SELECT c.id, c.name, c.wikipedia_url,\
                        c.wikiquote_url, c.metacritic_url, c.eztv_url\
                            FROM c OFFSET {offset} LIMIT {chunk_size}',
                    enable_cross_partition_query=True))

                if not shows:
                    break

                for show in shows:
                    save_json_to_local_temp_file(
                        show, filename='data_needed_for_detailed_scraper.ndjson')

                print(f'Got chunk starting with offset {offset}')
                offset += chunk_size
    except exceptions.CosmosHttpResponseError as e:
        print(f'Error during saving temp data locally. Error: {e.message}')
