import wikipediaapi
import json

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
    # fetch wikipedia titles from rdb and loop over them
    titles = ['Daredevil (TV series)']

    for title in titles:
        data = get_wikipedia_info(title)
        # then save to CosmoDB

####################################################################################


def start_scraper():
    get_detailed_info_for_all()
