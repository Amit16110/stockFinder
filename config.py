import os

class Config:
    SQLALCHEMY_DATABASE_URI = os.getenv(
        'DATABASE_URL', 'postgresql://root:secret@localhost:5432/stock'
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False
