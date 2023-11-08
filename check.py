# checks each catalog from stac index to check if it is public and valid
# afterward for each public and valid catalog, for each of their collection, run a stac
# search operation with AOI of each country in the list.
# save the results of each collection availability in the database
import json
import logging
import geopandas
import shapely
logging.basicConfig(level=logging.INFO)

from utils import *

if __name__ == "__main__":
    results = {}
    for catalog in get_list_of_public_catalogs():
        results[catalog["url"]] = {}

    logging.info(f"Found {len(results)} catalogs")

    for catalog_url in results.keys():
        logging.info(f"Checking {catalog_url} for collections")
        collections = get_collections_from_catalog_via_url(catalog_url)
        logging.info(f"Found {len(collections['collections'])} collections")
        for collection in collections["collections"]:
            results[catalog_url][collection["id"]] = {}

    world_borders_geodataframe = geopandas.read_file("World_Borders_subset.geojson")
    world_borders_geodataframe = world_borders_geodataframe.to_crs("EPSG:4326")

    for index, country in world_borders_geodataframe.iterrows():
        country_name = country["NAME"]
        country_geometry = country["geometry"]
        fips_code = country["FIPS"]
        shapely_multipolygon = shapely.geometry.shape(country_geometry)
        # create new shapely polygon from the envelope
        shapely_multipolygon_envelope = shapely.geometry.shape(shapely_multipolygon.envelope)

        logging.info(f"Country {country_name} has envelope {shapely_multipolygon_envelope.bounds}")

        for results_catalog_url in results.keys():
            for results_collection_id in results[results_catalog_url].keys():
                logging.info(f"Checking {results_catalog_url} for {results_collection_id} in {country_name} with "
                             f"envelope {shapely_multipolygon_envelope.bounds}")

                stac_search_endpoint = urljoin(results_catalog_url, f"search")
                stac_search_post_body = {
                    "collections": [results_collection_id],
                    "intersects": json.loads(shapely.to_geojson(shapely_multipolygon_envelope)),
                    "limit": 1
                }
                # make a post request to the stac search endpoint
                try:
                    logging.info(f"Making a POST request to {stac_search_endpoint} with body {stac_search_post_body}")
                    response = safe_request("POST", stac_search_endpoint, json=stac_search_post_body)
                    response.raise_for_status()
                    response_json = response.json()
                    if len(response_json["features"]) > 0:
                        results[results_catalog_url][results_collection_id][fips_code] = True
                        logging.info(
                            f"catalog {results_catalog_url} collection {results_collection_id} in {country_name} with "
                            f"envelope {shapely_multipolygon_envelope.bounds} has at least one asset")
                except (RequestException, JSONDecodeError) as e:
                    logging.error(
                        f"Failed to fetch or decode collections from {stac_search_endpoint}. Error: {e}"
                    )
                    raise e
