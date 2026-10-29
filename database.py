import os

from dotenv import load_dotenv
from sqlalchemy import create_engine


load_dotenv()


def get_db_engine():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL environment variable not set")

    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql+psycopg2://", 1)
    elif database_url.startswith("postgresql://"):
        database_url = database_url.replace("postgresql://", "postgresql+psycopg2://", 1)

    return create_engine(
        database_url,
        pool_pre_ping=True,
        pool_recycle=300,
        echo=False,
    )


db_engine = get_db_engine()
