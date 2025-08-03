from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

load_dotenv("config/settings.env")

DB_URL = os.getenv("DB_URL")

def test_db_connection():
    try:
        engine = create_engine(DB_URL)
        with engine.connect() as connection:
            result = connection.execute(text("SELECT sqlite_version();"))
            version = result.fetchone()
            print(f"✅ SQLite version: {version[0]}")
    except SQLAlchemyError as e:
        print(f"❌ Database connection failed: {e}")

if __name__ == "__main__":
    test_db_connection()