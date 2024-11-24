# session.py
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .models import Base

DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('MYSQL_USER')
DB_PASSWORD = os.getenv('MYSQL_PASSWORD')
DB_NAME = os.getenv('MYSQL_DATABASE')

if not all([DB_HOST, DB_USER, DB_PASSWORD, DB_NAME]):
    raise EnvironmentError("Missing required environment variables for database configuration.")

DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"

engine = create_engine(DATABASE_URL, echo=True)
SessionLocal = sessionmaker(bind=engine)

def init_db():
    """
    Initialize the database by creating all tables.
    """
    Base.metadata.create_all(bind=engine)
