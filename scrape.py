import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
import json

import geopandas
import shapely

from database import store_collection_in_database
from utils import *


def find_first_downloadable_asset_key(_assets: dict) -> str:
    """
    Find the first asset key that has a downloadable asset.

    Tries to find a first tif, tiff or nc file. If they are not present, the first asset key is returned.

    Args:
        _assets: Assets dictionary from a STAC item
    Returns: First asset key that has a downloadable asset

    """
    for asset_key, asset_info in _assets.items():
        asset_key_href = asset_info["href"].lower()
        if (
            asset_key_href.endswith(".tif")
            or asset_key_href.endswith(".tiff")
            or asset_key_href.endswith(".nc")
        ):
            return asset_key
    # If no asset with specific extensions is found, return the first asset key
    return list(_assets.keys())[0]


def check_if_stac_item_is_http_downloadable(_stac_item: dict) -> bool:
    """
    Check if a STAC item is downloadable using http.

    Args:
        _stac_item: STAC item dictionary from a STAC search response

    Returns: True if the STAC item is downloadable using http, False otherwise

    """
    try:
        asset_key = find_first_downloadable_asset_key(_stac_item["assets"])
        asset_href = _stac_item["assets"][asset_key]["href"]

        if not asset_href.startswith("http"):
            logging.info(f"Asset href does not start with http: {asset_href}")
            return False
        return True
    except KeyError:
        return False


def check_if_stac_item_is_http_directly_downloadable_without_token(
    _stac_item: dict,
) -> bool:
    """
    Check if a STAC item is downloadable using http without a token or some signing mechanism.

    Args:
        _stac_item: STAC item dictionary from a STAC search response

    Returns: True if the STAC item is downloadable using http without a token or some signing mechanism, False otherwise

    """
    asset_key = find_first_downloadable_asset_key(_stac_item["assets"])
    asset_href = _stac_item["assets"][asset_key]["href"]

    if not asset_href.startswith("http"):
        logging.info(f"Asset href does not start with http: {asset_href}")
        return False
    logging.info(f"Asset href: {asset_href}")

    # Do a HEAD request to the asset href to check if it is downloadable
    try:
        asset_response = safe_request("HEAD", asset_href)
        asset_response.raise_for_status()
        logging.info(f"Asset response: {asset_response.status_code}")
        return asset_response.status_code == 200
    except Exception as e:
        logging.error(f"Failed to do a HEAD request to {asset_href}. Error: {e}")
        return False


def check_if_sas_token_is_present_for_collection_on_mpc(_collection_id: str) -> tuple:
    """
    Check if a SAS token is present for a collection on the Planetary Computer.

    If the SAS token is present, return True and the URL to obtain the SAS token. If the SAS token is not present,
    return False and the URL to obtain the SAS token.

    Args:
        _collection_id: Collection ID

    Returns: Tuple of (True/False, URL to obtain the SAS token)

    """
    logger.info(f"Checking if collection {_collection_id} has available token")
    token_check_url = (
        f"https://planetarycomputer.microsoft.com/api/sas/v1/token/{_collection_id}"
    )
    try:
        token_check_response = safe_request("GET", token_check_url)
        token_check_response.raise_for_status()
        if token_check_response.status_code == 200:
            return True, token_check_url
        else:
            return False, token_check_url
    except Exception as e:
        return False, token_check_url


if __name__ == "__main__":
    # Section 1: Get List of Public Catalogs
    results = {catalog["url"]: {} for catalog in get_list_of_public_catalogs()}
    logging.info(f"Found {len(results)} catalogs")

    # Section 2: Retrieve Collections from Catalogs
    for catalog_url in results.keys():
        logging.info(f"Checking {catalog_url} for collections")
        collections = get_collections_from_catalog_via_url(catalog_url)
        logging.info(f"Found {len(collections['collections'])} collections")
        for collection in collections["collections"]:
            results[catalog_url][collection["id"]] = {}
    # results = {}
    # results["https://planetarycomputer.microsoft.com/api/stac/v1/"] = {}
    # results["https://planetarycomputer.microsoft.com/api/stac/v1/"]["landsat-c2-l2"] = {}

    # Section 3: Read World Borders GeoDataFrame
    world_borders_geodataframe = geopandas.read_file(
        "World_Borders_subset_bb_edit.geojson"
    )

    for index, country in world_borders_geodataframe.iterrows():
        country_name = country["NAME"]
        country_geometry = country["geometry"]
        fips_code = country["FIPS"]
        shapely_multipolygon = shapely.geometry.shape(country_geometry)
        # create new shapely polygon from the envelope
        shapely_multipolygon_envelope = shapely.geometry.shape(
            shapely_multipolygon.envelope
        )
        logging.info(
            f"Country {country_name} has envelope {shapely_multipolygon_envelope.bounds}"
        )

        for results_catalog_url in results.keys():
            for results_collection_id in results[results_catalog_url].keys():
                logging.info(
                    f"Checking {results_catalog_url} for {results_collection_id} in {country_name} with "
                    f"envelope {shapely_multipolygon_envelope.bounds}"
                )

                stac_search_endpoint = urljoin(results_catalog_url, f"search")
                stac_search_post_body = {
                    "collections": [results_collection_id],
                    "intersects": json.loads(
                        shapely.to_geojson(shapely_multipolygon_envelope)
                    ),
                    "limit": 1,
                }
                # make a post request to the stac search endpoint
                try:
                    logging.info(
                        f"Making a POST request to {stac_search_endpoint} with body {stac_search_post_body}"
                    )
                    response = safe_request(
                        "POST", stac_search_endpoint, json=stac_search_post_body
                    )
                    response.raise_for_status()
                    response_json = response.json()
                    if len(response_json["features"]) > 0:
                        logging.info(
                            f"catalog {results_catalog_url} collection {results_collection_id} in {country_name} with "
                            f"envelope {shapely_multipolygon_envelope.bounds} has at least one asset"
                        )

                        http_downloadable = False
                        requires_token = True
                        is_from_mpc = False
                        mpc_token_obtaining_url = ""

                        if "planetarycomputer" in results_catalog_url:
                            is_from_mpc = True

                        if check_if_stac_item_is_http_downloadable(
                            response_json["features"][0]
                        ):
                            http_downloadable = True
                            if check_if_stac_item_is_http_directly_downloadable_without_token(
                                response_json["features"][0]
                            ):
                                http_downloadable = True
                                requires_token = False
                            else:
                                if "planetarycomputer" in results_catalog_url:
                                    (
                                        token_present,
                                        token_url,
                                    ) = check_if_sas_token_is_present_for_collection_on_mpc(
                                        results_collection_id
                                    )
                                    if token_present:
                                        mpc_token_obtaining_url = token_url

                        # convert shapely_multipolygon_envelope to MultiPolygon if it is not multipolygon
                        if not isinstance(
                            shapely_multipolygon_envelope,
                            shapely.geometry.multipolygon.MultiPolygon,
                        ):
                            shapely_multipolygon_envelope = (
                                shapely.geometry.multipolygon.MultiPolygon(
                                    [shapely_multipolygon_envelope]
                                )
                            )

                        store_collection_in_database(
                            results_catalog_url,
                            results_collection_id,
                            shapely_multipolygon_envelope,
                            http_downloadable,
                            requires_token,
                            is_from_mpc,
                            mpc_token_obtaining_url,
                        )

                except (RequestException, JSONDecodeError) as e:
                    logging.error(
                        f"Failed to fetch or decode collections from {stac_search_endpoint}. Error: {e}"
                    )
                    continue
