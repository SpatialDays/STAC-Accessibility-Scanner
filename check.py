# checks each catalog from stac index to check if it is public and valid
# afterward for each public and valid catalog, for each of their collection, run a stac
# search operation with AOI of each country in the list.
# save the results of each collection availability in the database
import json
import logging
import geopandas
import shapely

logging.basicConfig(level=logging.INFO)

from database import Collection, session, ga

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
                        logging.info(
                            f"catalog {results_catalog_url} collection {results_collection_id} in {country_name} with "
                            f"envelope {shapely_multipolygon_envelope.bounds} has at least one asset")

                        http_downloadable = False
                        requires_token = True
                        try:
                            first_asset_key = list(response_json["features"][0]["assets"].keys())[0]
                            # logging.info(f"Asset key: {first_asset_key}")
                            first_asset_href = response_json["features"][0]["assets"][first_asset_key]["href"]
                            logging.info(f"Asset href: {first_asset_href}")
                        except KeyError as e:
                            logging.error(f"Failed to get asset href. Error: {e}")
                            continue

                        if first_asset_href.startswith("http"):
                            http_downloadable = True
                            # do a HEAD request to the asset href to check if it is downloadable
                            try:
                                asset_response = safe_request("HEAD", first_asset_href)
                                asset_response.raise_for_status()
                                logging.info(f"Asset response: {asset_response.status_code}")
                                if asset_response.status_code == 200:
                                    http_downloadable = True
                                    requires_token = False
                                else:
                                    http_downloadable = False
                                    requires_token = True
                            except Exception as e:
                                logging.error(f"Failed to do a HEAD request to {first_asset_href}. Error: {e}")
                                http_downloadable = True
                                requires_token = True
                        else:
                            http_downloadable = False
                            requires_token = True

                        # try and get a object from database with catalog_url, collection_id and spatial_extent
                        # if it exists, update the http_downloadable and requires_token fields
                        # if it doesn't exist, create a new object with catalog_url, collection_id, spatial_extent,
                        # http_downloadable and requires_token fields
                        collection_db_entry = session.query(Collection).filter(
                            Collection.catalog_url == results_catalog_url,
                            Collection.collection_id == results_collection_id,
                            fips_code == fips_code,
                            ga.functions.ST_Covers(Collection.spatial_extent,
                                                   ga.shape.from_shape(shapely_multipolygon_envelope, srid=4326))
                        ).first()
                        if collection_db_entry is None:
                            logging.info(f"Adding {results_catalog_url} {results_collection_id} to the database")
                            collection_db_entry = Collection()
                            collection_db_entry.catalog_url = results_catalog_url
                            collection_db_entry.collection_id = results_collection_id
                            collection_db_entry.fips_code = fips_code
                            collection_db_entry.spatial_extent = ga.shape.from_shape(shapely_multipolygon_envelope,
                                                                                     srid=4326)
                            collection_db_entry.http_downloadable = http_downloadable
                            collection_db_entry.requires_token = requires_token
                            session.add(collection_db_entry)
                            session.commit()
                            logging.info(f"Added {results_catalog_url} {results_collection_id} to the database")
                        else:
                            logging.info(f"Updating {results_catalog_url} {results_collection_id} in the database")
                            collection_db_entry.http_downloadable = http_downloadable
                            collection_db_entry.requires_token = requires_token
                            session.commit()
                            logging.info(f"Updated {results_catalog_url} {results_collection_id} in the database")

                        logging.info(f"Added {results_catalog_url} {results_collection_id} to the database")

                except (RequestException, JSONDecodeError) as e:
                    logging.error(
                        f"Failed to fetch or decode collections from {stac_search_endpoint}. Error: {e}"
                    )
                    continue
