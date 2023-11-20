import logging
import json

logger = logging.getLogger(__name__)
import os
import flask
from flask_cors import CORS
from dotenv import load_dotenv

import geoalchemy2 as ga
from database import session, Collection
from urllib.parse import urljoin

from shapely.geometry import shape
from flask import request, jsonify
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
@app.route("/get_collections/", methods=["POST"])
def get_collections():
    data = request.json
    aoi = data.get("aoi")
    is_available_from_mpc = data.get("is_available_from_mpc")
    if not aoi:
        return {"error": "aoi is required"}, 400

    aoi_shapely = shape(aoi)
    collections = session.query(Collection).filter(
        ga.functions.ST_Intersects(
            Collection.spatial_extent, ga.shape.from_shape(aoi_shapely, srid=4326)
        )
    )

    # Apply filters directly from request data
    for key, value in data.items():
        if key in Collection.__table__.columns and value is not None:
            collections = collections.filter(getattr(Collection, key) == value)
            
    if is_available_from_mpc:
        collections = collections.filter(
            (Collection.is_from_mpc == False) | 
            ((Collection.is_from_mpc == True) & (Collection.mpc_token_obtaining_url != None))
        )
    collections = collections.all()

    results = {}
    for i in collections:
        aoi_as_shapely = ga.shape.to_shape(i.spatial_extent)
        aoi_as_geojson = json.loads(shapely.to_geojson(aoi_as_shapely))
        results[i.collection_id] = {
            "catalog_url": i.catalog_url,
            "http_downloadable": i.http_downloadable,
            "requires_token": i.requires_token,
            "is_from_mpc": i.is_from_mpc,
            "mpc_token_obtaining_url": i.mpc_token_obtaining_url,
            "collection_stac_url": urljoin(
                i.catalog_url, f"collections/{i.collection_id}"
            ),
            "aoi": aoi_as_geojson,
        }
    return flask.jsonify(results), 200


if __name__ == "__main__":
    app.run(host=APP_HOST, port=int(APP_PORT), debug=True)
