from sqlalchemy import orm
import sqlalchemy as sa
import geoalchemy2 as ga

base = sa.orm.declarative_base()
# engine = sa.create_engine('sqlite:///db.sqlite')
engine = sa.create_engine("postgresql://postgres:postgres@localhost:15432/stacaccessibility_db")
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


if __name__ == '__main__':
    base.metadata.create_all(engine)
