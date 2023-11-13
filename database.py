import logging

logger = logging.getLogger(__name__)
import os
import geoalchemy2 as ga
import shapely
import sqlalchemy as sa
from sqlalchemy import orm
from dotenv import load_dotenv

load_dotenv()

DATABASE_HOST = os.getenv("DATABASE_HOST")
DATABASE_PORT = os.getenv("DATABASE_PORT")
DATABASE_NAME = os.getenv("DATABASE_NAME")
DATABASE_USER = os.getenv("DATABASE_USER")
DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD")



base = sa.orm.declarative_base()
# engine = sa.create_engine('sqlite:///db.sqlite')
# engine = sa.create_engine("postgresql://postgres:postgres@localhost:15432/stacaccessibility_db")
engine = sa.create_engine(
    f"postgresql://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}")
base.metadata.bind = engine
session = orm.scoped_session(orm.sessionmaker())(bind=engine)


class Collection(base):
    __tablename__ = 'collections'
    catalog_url = sa.Column(sa.String)
    collection_id = sa.Column(sa.String)
    spatial_extent = sa.Column(ga.Geometry('MULTIPOLYGON'))
    # make a composite primary key from catalog_url, collection_id and spatial_extent
    sa.PrimaryKeyConstraint(catalog_url, collection_id, spatial_extent)
    http_downloadable = sa.Column(sa.Boolean)
    requires_token = sa.Column(sa.Boolean)
    is_from_mpc = sa.Column(sa.Boolean, default=False)
    mpc_token_obtaining_url = sa.Column(sa.String, default="")


def store_collection_in_database(
        _catalog_url: str,
        _collection_id: str,
        _spatial_extent: shapely.geometry.multipolygon.MultiPolygon,
        _http_downloadable: bool,
        _requires_token: bool,
        _is_from_mpc: bool = False,
        _mpc_token_obtaining_url: str = "",
) -> None:
    """
    Store a collection in the database.

    If the same entry is already present in the database with the composite primary key, update the entry.

    Args:
        _catalog_url: STAC catalog URL
        _collection_id: Collection ID
        _spatial_extent: Shapely object of the spatial extent
        _http_downloadable: Flag indicating if the items in the collection are http downloadable
        _requires_token: Flag indicating if the items in the collection require a token or some signing mechanism
        _is_from_mpc: Flag indicating if the collection is from the Planetary Computer
        _mpc_token_obtaining_url: URL to obtain the token for the collection from the Planetary Computer

    Returns: None
    """
    collection_db_entry = (
        session.query(Collection)
        .filter(
            Collection.catalog_url == _catalog_url,
            Collection.collection_id == _collection_id,
            ga.functions.ST_Covers(
                Collection.spatial_extent,
                ga.shape.from_shape(_spatial_extent, srid=4326),
            ),
        )
        .first()
    )
    if collection_db_entry is None:
        logger.debug(f"Adding {_catalog_url} {_collection_id} to the database")
        collection_db_entry = Collection()
        collection_db_entry.catalog_url = _catalog_url
        collection_db_entry.collection_id = _collection_id
        collection_db_entry.spatial_extent = ga.shape.from_shape(
            _spatial_extent, srid=4326
        )
        collection_db_entry.http_downloadable = _http_downloadable
        collection_db_entry.requires_token = _requires_token
        collection_db_entry.is_from_mpc = _is_from_mpc
        collection_db_entry.mpc_token_obtaining_url = _mpc_token_obtaining_url
        session.add(collection_db_entry)
        session.commit()
        logger.info(f"Added {_catalog_url} {_collection_id} to the database")
    else:
        logger.debug(f"Updating {_catalog_url} {_collection_id} in the database")
        collection_db_entry.http_downloadable = _http_downloadable
        collection_db_entry.requires_token = _requires_token
        collection_db_entry.is_from_mpc = _is_from_mpc
        collection_db_entry.mpc_token_obtaining_url = _mpc_token_obtaining_url
        session.commit()
        logger.info(f"Updated {_catalog_url} {_collection_id} in the database")


if __name__ == '__main__':
    plugin_enable_statement =  sa.text("CREATE EXTENSION IF NOT EXISTS postgis;")
    with engine.connect() as conn:
        conn.execute(plugin_enable_statement)
        logger.info("Enabled postgis extension")
        print("Enabled postgis extension")
    base.metadata.create_all(engine)
