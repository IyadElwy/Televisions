import requests
from time import sleep


detailed_single_search_base_url = 'https://api.tvmaze.com/singlesearch/shows?q={}{}'

detailed_retrieve_by_id_base_url = 'https://api.tvmaze.com/shows/{}{}'

season_detailed_retrieve_by_id_base_url = 'https://api.tvmaze.com/shows/{}/seasons'


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


def detailed_retrieve_by_id(query, **kwargs):
    """
    embed[]=episodes&embed[]=cast
    """

    if kwargs:
        url = detailed_retrieve_by_id_base_url.format(
            query,
            ''.join(['&embed[]=' + key for key in kwargs if kwargs[key]]))
        response = requests.get(url)
        assert response.status_code == 200

        return response.json()

    else:
        url = detailed_retrieve_by_id_base_url.format(query, '')
        response = requests.get(url)
        assert response.status_code == 200

        return response.json()


def season_detailed_retrieve_by_id(query):
    url = season_detailed_retrieve_by_id_base_url.format(query)
    response = requests.get(url)
    assert response.status_code == 200

    return response.json()
