from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

class Collection(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    url = db.Column(db.Text, nullable=False)
    accessible = db.Column(db.Boolean)
