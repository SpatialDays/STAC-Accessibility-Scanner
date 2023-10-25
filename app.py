# Standard library imports
import os
from json import JSONDecodeError
from urllib.parse import urljoin

# Third-party library imports
import logging
from flask import Flask, jsonify
from requests.exceptions import RequestException

# Local application/library specific imports
from models import Collection, db
from utils import safe_request

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
    return f"There are {count} collections in the database."


@app.route("/check-catalogs")
def check_catalogs():
    try:
        check_publicly_available_catalogs()
        return jsonify({"message": "Catalogs checked successfully."}), 200
    except Exception as e:
        logging.error(f"Failed to check catalogs. Error: {e}")
        return jsonify({"message": "Failed to check catalogs."}), 500


def is_collection_public_and_valid(collection: dict) -> bool:
    """
    Check if a collection has assets and that one of the assets is publicly downloadable.
    """
    # Find the link with the rel type 'items'
    items_link = next(
        (link["href"] for link in collection["links"] if link["rel"] == "items"), None
    )

    if not items_link:
        logging.info(f"-- No items endpoint found for collection {collection['id']}")
        return False

    logging.info(f"-- Items endpoint - {items_link}")

    items_response = safe_request("GET", items_link)

    # Check the HTTP Status Code
    if items_response.status_code != 200:
        logging.error(
            f"Failed to fetch items from {items_link}. Status code: {items_response.status_code}"
        )
        return False

    try:
        items = items_response.json()
    except JSONDecodeError:
        logging.error(
            f"Failed to decode JSON from {items_link}. Response content: {items_response.text}"
        )
        return False

    # Check for the presence of features and if there's at least one feature in the list
    if "features" not in items or not items["features"]:
        logging.info(f"-- No features found for collection {collection['id']}")
        return False

    # Get the first feature
    first_feature = items["features"][0]

    # Check for assets in the first feature
    if "assets" not in first_feature or not first_feature["assets"]:
        logging.info(
            f"-- No assets found for the first feature of collection {collection['id']}"
        )
        return False

    first_feature_assets = first_feature["assets"]

    # Get the href of the first asset
    asset_name = next(iter(first_feature_assets))
    asset_url = first_feature_assets[asset_name]["href"]
    logging.info(f"-- Asset URL - {asset_url}")

    # Check the asset for its accessibility with a HEAD request to avoid downloading the entire asset
    try:
        asset_response = safe_request("HEAD", asset_url)
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
    try:
        response = safe_request("GET", collection_url)
        response.raise_for_status()  # This will raise an HTTPError if the HTTP request returned an unsuccessful status code
        collections = response.json()
        return collections
    except (RequestException, JSONDecodeError) as e:
        logging.error(
            f"Failed to fetch or decode collections from {collection_url}. Error: {e}"
        )
        return None


def check_publicly_available_catalogs():
    """
    Check all publicly available catalogs and loop through their collections to see if they are valid.
    """
    successful_collections = []
    unsuccessful_collections = []
    lookup_api = "https://stacindex.org/api/catalogs"
    try:
        response = safe_request("GET", lookup_api)
        response.raise_for_status()
        catalogs = response.json()
    except (RequestException, JSONDecodeError) as e:
        logging.error(f"Failed to fetch or decode catalogs from STAC INDEX. Error: {e}")
        return

    filtered_catalogs = [i for i in catalogs if not i["isPrivate"] and i["isApi"]]

    for catalog in filtered_catalogs:
        logging.info(f"Catalog - {catalog['title']}")
        # # for testing see if title starts with FedEO
        # if not catalog["title"].startswith("FedEO"):
        #     continue
        collections = return_collections(catalog["url"])
        if not collections or "collections" not in collections:
            logging.info(f"No collections found for catalog {catalog['title']}")
            continue

        for collection in collections["collections"]:
            logging.info(f"Collection - {collection['id']}")
            accessible = is_collection_public_and_valid(collection)

            # Check if the collection already exists in the database
            existing_collection = Collection.query.filter_by(
                name=collection["id"]
            ).first()
            if existing_collection:
                # Update the existing record
                existing_collection.accessible = accessible
                db.session.commit()
            else:
                # Add a new record
                new_collection = Collection(
                    name=collection["id"],
                    url=collection["links"][0]["href"],
                    accessible=accessible,
                )
                db.session.add(new_collection)
                db.session.commit()

            if accessible:
                successful_collections.append(collection["id"])
            else:
                unsuccessful_collections.append(collection["id"])

    logging.info(f"Successful collections - {successful_collections}")
    logging.info(f"Unsuccessful collections - {unsuccessful_collections}")


if __name__ == "__main__":
    with app.app_context():
        app.run(debug=True, host="0.0.0.0")
