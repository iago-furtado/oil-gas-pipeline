from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Load environment variables
load_dotenv("config/settings.env")
DB_URL = os.getenv("DB_URL")

# DDL statements split manually
ddl_statements = [
    """
    CREATE TABLE IF NOT EXISTS production (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date DATE NOT NULL,
        state TEXT NOT NULL,
        oil_volume FLOAT,
        gas_volume FLOAT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS wells (
        id TEXT PRIMARY KEY,
        well_name TEXT,
        operator_name TEXT,
        status TEXT,
        county TEXT,
        latitude FLOAT,
        longitude FLOAT
    )
    """
]

def init_db():
    try:
        engine = create_engine(DB_URL)
        with engine.connect() as connection:
            for ddl in ddl_statements:
                connection.execute(text(ddl))
            print("Tables created successfully.")
    except SQLAlchemyError as e:
        print(f"Failed to initialize database: {e}")

if __name__ == "__main__":
    init_db()