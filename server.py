import logging
import json

logger = logging.getLogger(__name__)
import os
import flask
from flask_cors import CORS
from dotenv import load_dotenv

import geoalchemy2 as ga
from sqlalchemy import or_, and_
from database import session, Collection
from urllib.parse import urljoin

from shapely import to_geojson
from shapely.geometry import shape
from flask import request
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
    data = request.get_json()
    aoi = data.get("aoi")
    public = data.get("public")
    mpc_with_token = data.get("mpc_with_token")

    aoi_shapely = shape(aoi)
    collections = (
        session.query(Collection)
        .filter(
            ga.functions.ST_Intersects(
                Collection.spatial_extent, ga.shape.from_shape(aoi_shapely, srid=4326)
            )
        )
        .distinct()
    )

    conditions = []
    if public or mpc_with_token:
        if public:
            conditions.append(
                and_(
                    Collection.http_downloadable == True,
                    Collection.requires_token == False,
                )
            )
        if mpc_with_token:
            conditions.append(
                and_(Collection.requires_token == True, Collection.is_from_mpc == True)
            )

    collections = collections.filter(or_(*conditions))
    collection_results = collections.all()    
    
    response_data = {}
    for i in collection_results:
        aoi_as_shapely = ga.shape.to_shape(i.spatial_extent)
        aoi_as_geojson = json.loads(to_geojson(aoi_as_shapely))
        response_data[i.collection_id] = {
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
    return flask.jsonify(response_data), 200


if __name__ == "__main__":
    app.run(host=APP_HOST, port=int(APP_PORT), debug=True)
