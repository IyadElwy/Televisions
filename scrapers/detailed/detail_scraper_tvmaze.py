import requests
import aiohttp
import asyncio


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


async def async_get_and_merge_info(title):
    single_search_result = await async_detailed_single_search(title)
    season_search_result = await async_season_detailed_retrieve_by_id(single_search_result['id'])

    single_search_result['seasons'] = season_search_result

    # save to typesense


async def async_get_detailed_info_about_all():
    tasks = []

    # get titles from typesense titles attribute and loop over them

    titles = ['daredevil']
    for title in titles:
        tasks.append(async_get_and_merge_info(title))

    return await asyncio.gather(*tasks)

####################################################################################


def start_scraper():
    asyncio.run(async_get_detailed_info_about_all())
