import json
import logging
from urllib.parse import urljoin
from requests.exceptions import RequestException
from json.decoder import JSONDecodeError
import geopandas
import shapely

logging.basicConfig(level=logging.INFO)

from database import Collection, session, ga
from utils import *


def store_collection_in_database(
    catalog_url: str,
    collection_id: str,
    spatial_extent: shapely.geometry.multipolygon.MultiPolygon,
    http_downloadable: bool,
    requires_token: bool,
    is_from_mpc: bool = False,
    mpc_token_obtaining_url: str = "",
):
    """
    Store or update a collection in the database.
    """
    collection_db_entry = (
        session.query(Collection)
        .filter(
            Collection.catalog_url == catalog_url,
            Collection.collection_id == collection_id,
            ga.functions.ST_Covers(
                Collection.spatial_extent,
                ga.shape.from_shape(spatial_extent, srid=4326),
            ),
        )
        .first()
    )
    if collection_db_entry is None:
        logging.info(f"Adding {catalog_url} {collection_id} to the database")
        collection_db_entry = Collection()
        collection_db_entry.catalog_url = catalog_url
        collection_db_entry.collection_id = collection_id
        collection_db_entry.spatial_extent = ga.shape.from_shape(
            spatial_extent, srid=4326
        )
        collection_db_entry.http_downloadable = http_downloadable
        collection_db_entry.requires_token = requires_token
        collection_db_entry.is_from_mpc = is_from_mpc
        collection_db_entry.mpc_token_obtaining_url = mpc_token_obtaining_url
        session.add(collection_db_entry)
        session.commit()
        logging.info(f"Added {catalog_url} {collection_id} to the database")
    else:
        logging.info(f"Updating {catalog_url} {collection_id} in the database")
        collection_db_entry.http_downloadable = http_downloadable
        collection_db_entry.requires_token = requires_token
        collection_db_entry.is_from_mpc = is_from_mpc
        collection_db_entry.mpc_token_obtaining_url = mpc_token_obtaining_url
        session.commit()
        logging.info(f"Updated {catalog_url} {collection_id} in the database")

    logging.info(f"Added {catalog_url} {collection_id} to the database")

import logging

def find_first_downloadable_asset_key(assets: dict):
    """
    Find the first asset key where href ends with specific extensions.
    """
    for asset_key, asset_info in assets.items():
        asset_key_href = asset_info["href"].lower()
        if (
            asset_key_href.endswith(".tif")
            or asset_key_href.endswith(".tiff")
            or asset_key_href.endswith(".nc")
        ):
            return asset_key
    # If no asset with specific extensions is found, return the first asset key
    return list(assets.keys())[0]

def check_if_stac_item_is_http_downloadable(stac_item: dict):
    """
    Check if a STAC item is http downloadable.
    """
    asset_key = find_first_downloadable_asset_key(stac_item["assets"])
    asset_href = stac_item["assets"][asset_key]["href"]

    if not asset_href.startswith("http"):
        logging.info(f"Asset href does not start with http: {asset_href}")
        return False
    return True

def check_if_stac_item_is_http_directly_downloadable_without_token(stac_item: dict):
    """
    Check if a STAC item is downloadable.
    """
    asset_key = find_first_downloadable_asset_key(stac_item["assets"])
    asset_href = stac_item["assets"][asset_key]["href"]

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


def check_if_sas_token_is_present_for_collection_on_mpc(collection_id:str):
    logger.info(
        f"Checking if collection {collection_id} has available token"
    )
    token_check_url = f"https://planetarycomputer.microsoft.com/api/sas/v1/token/{collection_id}"
    try:
        token_check_response = safe_request(
            "GET", token_check_url
        )
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

                        if check_if_stac_item_is_http_downloadable(response_json["features"][0]):
                            http_downloadable = True
                            if check_if_stac_item_is_http_directly_downloadable_without_token(response_json["features"][0]):
                                http_downloadable = True
                                requires_token = False
                            else:
                                requires_token = True
                                if "planetarycomputer" in results_catalog_url:
                                    is_from_mpc = True
                                    token_present, token_url = check_if_sas_token_is_present_for_collection_on_mpc(results_collection_id)
                                    if token_present:
                                        mpc_token_obtaining_url = token_url


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
