from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


class Collection(db.Model):
    # id = db.Column(db.Integer, primary_key=True)
    # name = db.Column(db.String(255), nullable=False)
    # url = db.Column(db.Text, nullable=False)
    # accessible = db.Column(db.Boolean)
    catalog_url = db.Column(db.Text)
    collection_id = db.Column(db.Text)
    # make a composite primary key
    __table_args__ = (db.PrimaryKeyConstraint("catalog_url", "collection_id"),)
    http_download = db.Column(db.Boolean)
    signing_required = db.Column(db.Boolean)
