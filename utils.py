import logging

logger = logging.getLogger(__name__)

import time
from urllib.parse import urljoin
from json import JSONDecodeError
from requests.exceptions import RequestException
import requests


def safe_request(method, url, max_retries=3, backoff_factor=1, **kwargs):
    """
    Make a request, with added handling for 429 Too Many Requests.
    Retry the request up to `max_retries` times, waiting `backoff_factor` seconds between retries.
    """
    for attempt in range(max_retries):
        response = requests.request(method, url, **kwargs)
        if response.status_code != 429:  # Not rate-limited
            return response
        # If rate-limited, wait before retrying
        time.sleep(backoff_factor * (attempt + 1))
    return response


def get_collections_from_catalog_via_url(catalog_url: str):
    """
    Return all collections from a catalog.
    """
    collection_url = urljoin(catalog_url, "collections")
    try:
        response = safe_request("GET", collection_url)
        response.raise_for_status()  # This will raise an HTTPError if the HTTP request returned an unsuccessful
        # status code
        collections = response.json()
        return collections
    except (RequestException, JSONDecodeError) as e:
        logging.error(
            f"Failed to fetch or decode collections from {collection_url}. Error: {e}"
        )
        raise e


def get_list_of_public_catalogs():
    """
    Check all publicly available catalogs and loop through their collections to see if they are valid.
    """
    lookup_api = "https://stacindex.org/api/catalogs"
    try:
        response = safe_request("GET", lookup_api)
        response.raise_for_status()
        catalogs = response.json()
    except (RequestException, JSONDecodeError) as e:
        logging.error(f"Failed to fetch or decode catalogs from STAC INDEX. Error: {e}")
        return

    filtered_catalogs = [i for i in catalogs if not i["isPrivate"] and i["isApi"]]
    catalogs_with_working_search = []
    for i in filtered_catalogs:
        search_url = urljoin(i["url"], "search")
        search_body = {
            "limit": 1,
            "intersects": {
                "type": "Polygon",
                "coordinates": [
                    [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
                ],
            },
        }
        logger.info(f"Checking search on {search_url}")
        try:
            response = safe_request("POST", search_url, json=search_body)
            response.raise_for_status()
            catalogs_with_working_search.append(i)
            logging.info(f"Catalog {i['url']} has a working search endpoint")
        except (RequestException, JSONDecodeError) as e:
            logger.error(
                f"Failed to fetch or decode search from {search_url}. Error: {e}"
            )
            continue
        # return catalogs_with_working_search  # just for quick testing

    return catalogs_with_working_search
