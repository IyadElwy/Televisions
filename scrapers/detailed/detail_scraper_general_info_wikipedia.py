import wikipediaapi
import json

from utils.merging_with_cosmosDB import update_item_with_attribute
from services.azure_cosmos_db import CosmosDbConnection

wikipedia_searcher = wikipediaapi.Wikipedia('wikipedia api', 'en')

####################################################################################


def format_text_to_sections(text):
    result_dict = {}

    split_text = text.split('\n')

    current_section = 'summary'
    for index, string in enumerate(split_text):
        if current_section not in result_dict:
            result_dict[current_section] = ''

        if string == '' and index + 1 <= len(split_text):
            current_section = split_text[index + 1]
        else:
            result_dict[current_section] += string

    return result_dict


def get_wikipedia_info(title):
    print(f'Starting page: {title}')

    page = wikipedia_searcher.page(title)

    if not page.exists():
        print({'not_found': title,
               })
        return {title: 'page not found'}

    title = page.title
    text = page.text
    url = page.fullurl

    output = {'title': title,
              'url': url,
              'sections': format_text_to_sections(text)}

    print(f'Page done for {title}')
    return output


def get_detailed_info_for_all():
    with open('temp/data_needed_for_detailed_scraper.ndjson', 'r') as file:
        with CosmosDbConnection() as conn:

            for line in file:
                parsed_info = json.loads(line)
                id = parsed_info['id']
                if 'wikipedia_url' not in parsed_info or not parsed_info['wikipedia_url']:
                    continue
                wikipedia_url = parsed_info['wikipedia_url']
                slug = wikipedia_url.split('wiki/')
                if len(slug) == 0:
                    continue
                title = slug[-1].replace('-', ' ').replace('_', ' ')
                data = get_wikipedia_info(title)
                update_item_with_attribute(
                    conn, parsed_info['id'], 'wikipedia', data)
                print(
                    f'Updated data on CosmosDB with wikipedia data show with id: {id}')


####################################################################################


def start_scraper():
    get_detailed_info_for_all()
