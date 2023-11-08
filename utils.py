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


# def is_collection_public_and_valid(collection: dict) -> bool:
#     """
#     Check if a collection has assets and that one of the assets is publicly downloadable.
#     """
#     # Find the link with the rel type 'items'
#     items_link = next(
#         (link["href"] for link in collection["links"] if link["rel"] == "items"), None
#     )
#
#     if not items_link:
#         logger.info(f"-- No items endpoint found for collection {collection['id']}")
#         return False
#
#     logger.info(f"-- Items endpoint - {items_link}")
#
#     items_response = safe_request("GET", items_link)
#
#     # Check the HTTP Status Code
#     if items_response.status_code != 200:
#         logger.error(
#             f"Failed to fetch items from {items_link}. Status code: {items_response.status_code}"
#         )
#         return False
#
#     try:
#         items = items_response.json()
#     except JSONDecodeError:
#         logger.error(
#             f"Failed to decode JSON from {items_link}. Response content: {items_response.text}"
#         )
#         return False
#
#     # Check for the presence of features and if there's at least one feature in the list
#     if "features" not in items or not items["features"]:
#         logger.info(f"-- No features found for collection {collection['id']}")
#         return False
#
#     # Get the first feature
#     first_feature = items["features"][0]
#
#     # Check for assets in the first feature
#     if "assets" not in first_feature or not first_feature["assets"]:
#         logger.info(
#             f"-- No assets found for the first feature of collection {collection['id']}"
#         )
#         return False
#
#     first_feature_assets = first_feature["assets"]
#
#     # Get the href of the first asset
#     asset_name = next(iter(first_feature_assets))
#     asset_url = first_feature_assets[asset_name]["href"]
#     logger.info(f"-- Asset URL - {asset_url}")
#
#     # Check the asset for its accessibility with a HEAD request to avoid downloading the entire asset
#     try:
#         asset_response = safe_request("HEAD", asset_url)
#         logger.info(f"-- Asset response - {asset_response.status_code}")
#     except Exception as e:
#         logger.info(f"-- Asset response - {e}")
#         return False
#
#     return asset_response.status_code == 200


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
    return [{
        "url" : "https://planetarycomputer.microsoft.com/api/stac/v1/"
    }]
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
                    [
                        [
                            -180,
                            -90
                        ],
                        [
                            180,
                            -90
                        ],
                        [
                            180,
                            90
                        ],
                        [
                            -180,
                            90
                        ],
                        [
                            -180,
                            -90
                        ]
                    ]
                ]
            }
        }
        logger.info(f"Checking search on {search_url}")
        try:
            response = safe_request("POST", search_url, json=search_body)
            response.raise_for_status()
            catalogs_with_working_search.append(i)
            logging.info(f"Catalog {i['url']} has a working search endpoint")
        except (RequestException, JSONDecodeError) as e:
            logger.error(f"Failed to fetch or decode search from {search_url}. Error: {e}")
            continue
        # return catalogs_with_working_search  # just for quick testing

    return catalogs_with_working_search
