import logging
import json

logger = logging.getLogger(__name__)
import os
import flask
from flask_cors import CORS
from dotenv import load_dotenv
import shapely
import geoalchemy2 as ga
from database import session, Collection
from urllib.parse import urljoin

load_dotenv()
APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
APP_PORT = os.getenv("APP_PORT", "5000")
APP_DEBUG = os.getenv("APP_DEBUG", "True") == "True"

app = flask.Flask(__name__)
CORS(app)


# Create /healthz endpoint
@app.route("/healthz")
def healthz():
    return flask.Response(status=200)


# Make a POST endpoint which will take catalog_url and aoi
# in geojson format and filter the database for available collections
@app.route("/get_collections", methods=["POST"])
def get_collections():
    aoi = flask.request.json.get("aoi", None)
    if not aoi:
        # send 400 bad request with message that aoi is required
        return {"error": "aoi is required"}, 400
    
    catalog_url = flask.request.json.get("catalog_url", None)
    collection_id = flask.request.json.get("collection_id", None)
    aoi_shapely = shapely.geometry.shape(aoi)
    collections = session.query(Collection).filter(
        ga.functions.ST_Intersects(
            Collection.spatial_extent, ga.shape.from_shape(aoi_shapely, srid=4326)
        ),
    )

    if catalog_url:
        collections = collections.filter(Collection.catalog_url == catalog_url)

    if collection_id:
        collections = collections.filter(Collection.collection_id == collection_id)

    collections = collections.all()

    results = {}
    for i in collections:
        aoi_as_shapely = shapely.geometry.shape(aoi)
        aoi_as_geojson = json.loads(shapely.to_geojson(aoi_as_shapely))
        results[i.collection_id] = {
            "catalog_url": i.catalog_url,
            "http_downloadable": i.http_downloadable,
            "requires_token": i.requires_token,
            "is_from_mpc": i.is_from_mpc,
            "mpc_token_obtaining_url": i.mpc_token_obtaining_url,
            "collection_stac_url": urljoin(i.catalog_url, f"collections/{i.collection_id}"),
            "aoi": aoi_as_geojson,
        }
    return flask.jsonify(results), 200


if __name__ == "__main__":
    app.run(host=APP_HOST, port=int(APP_PORT), debug=True)
