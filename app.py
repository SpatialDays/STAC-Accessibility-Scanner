from json import JSONDecodeError
import os
import logging
import requests
from flask import Flask
from models import Collection, db
from urllib.parse import urljoin

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get("DATABASE_URL")
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db.init_app(app)

# logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s : %(message)s",
)


@app.route("/")
def index():
    logging.info("Index route")
    count = db.session.query(Collection).count()
    check_publicly_available_catalogs()
    return f"Hello, world! There are {count} collections in the database."


def is_collection_public_and_valid(collection: dict) -> bool:
    """
    Check if a collection has assets and that one of the assets is publicly downloadable.
    """
    # Find the link with the rel type 'items'
    items_link = [link for link in collection["links"] if link["rel"] == "items"][0]["href"]
    logging.info(f"-- Items endpoint - {items_link}")
    
    items_response = requests.get(items_link)

    # Check the HTTP Status Code
    if items_response.status_code != 200:
        logging.error(f"Failed to fetch items from {items_link}. Status code: {items_response.status_code}")
        return False

    try:
        items = items_response.json()
    except JSONDecodeError:
        logging.error(f"Failed to decode JSON from {items_link}. Response content: {items_response.text}")
        return False

    # Check for features
    if not items.get("features"):
        logging.info(f"-- No features found for collection {collection['id']}")
        return False

    # Get the first feature and check for assets
    first_feature_assets = items["features"][0]["assets"]

    if not first_feature_assets:
        logging.info(
            f"-- No assets found for the first feature of collection {collection['id']}"
        )
        return False

    # Get the href of the first asset
    asset_name = next(iter(first_feature_assets))
    asset_url = first_feature_assets[asset_name]["href"]
    logging.info(f"-- Asset URL - {asset_url}")

    # Check the asset for its accessibility with a HEAD request to avoid downloading the entire asset
    try:
        asset_response = requests.head(asset_url)
        logging.info(f"-- Asset response - {asset_response.status_code}")
    except Exception as e:
        logging.info(f"-- Asset response - {e}")
        return False

    return asset_response.status_code == 200


def return_collections(catalog_url: str):
    """
    Return all collections from a catalog.
    """
    collection_url = urljoin(catalog_url, "collections")
    response = requests.get(collection_url)

    # Error handling and checks to ensure the response contains valid JSON might be added here.
    collections = response.json()

    return collections


def check_publicly_available_catalogs():
    """
    Check all publicly available catalogs and loop through their collections to see if they are valid.
    """
    successful_collections = []
    unsuccessful_collections = []
    lookup_api: str = "https://stacindex.org/api/catalogs"
    logging.info(f"STAC INDEX - {lookup_api}")
    response = requests.get(lookup_api)
    catalogs = response.json()
    filtered_catalogs = [i for i in catalogs if not i["isPrivate"] and i["isApi"]]

    for catalog in filtered_catalogs:
        logging.info(f"Catalog - {catalog['title']}")
        # if not catalog["title"].startswith("CBERS"):
        #     continue

        collections = return_collections(catalog["url"])
        if not collections or 'collections' not in collections:
            logging.info(f"No collections found for catalog {catalog['title']}")
            continue

        for collection in collections['collections']:
            logging.info(f"Collection - {collection['id']}")
            if is_collection_public_and_valid(collection):
                successful_collections.append(collection["id"])
            else:
                unsuccessful_collections.append(collection["id"])

    logging.info(f"Successful collections - {successful_collections}")
    logging.info(f"Unsuccessful collections - {unsuccessful_collections}")


if __name__ == "__main__":
    with app.app_context():
        app.run(debug=True, host="0.0.0.0")
