import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# Load environment variables
load_dotenv("config/settings.env")
DB_URL = os.getenv("DB_URL")

# Create SQLAlchemy engine
engine = create_engine(DB_URL)

def run_query(sql_query):
    try:
        with engine.connect() as connection:
            df = pd.read_sql(sql_query, connection)
            return df
    except SQLAlchemyError as e:
        print("Error executing query:", e)
        return None

def main():
    # Query 1
    query1 = """
    SELECT
        date,
        SUM(oil_volume) AS total_oil,
        SUM(gas_volume) AS total_gas
    FROM production
    WHERE state = 'West Virginia'
      AND date >= DATE('now', '-12 months')
    GROUP BY date
    ORDER BY date;
    """
    df1 = run_query(query1)
    if df1 is not None:
        print("Query 1: Total oil and gas production for West Virginia (last 12 months):")
        print(df1.head(), "\n")

    # Query 2
    query2 = """
    SELECT
        county,
        COUNT(*) AS well_count
    FROM wells
    GROUP BY county
    ORDER BY well_count DESC
    LIMIT 1;
    """
    df2 = run_query(query2)
    if df2 is not None:
        print("Query 2: County with the highest number of wells:")
        print(df2.head(), "\n")

    # Query 4 - YoY production variation (no LAG, using CTE)
    query4 = """
WITH yearly_production AS (
    SELECT
        STRFTIME('%Y', date) AS year,
        SUM(oil_volume) AS total_oil,
        SUM(gas_volume) AS total_gas
    FROM production
    GROUP BY year
    ),
    yoy_comparison AS (
        SELECT
            curr.year,
            curr.total_oil,
            curr.total_gas,
            prev.total_oil AS previous_oil,
            prev.total_gas AS previous_gas,
            ROUND( ((curr.total_oil - prev.total_oil)*1.0 / prev.total_oil) * 100, 2) AS oil_yoy_change_percent,
            ROUND( ((curr.total_gas - prev.total_gas)*1.0 / prev.total_gas) * 100, 2) AS gas_yoy_change_percent
        FROM yearly_production curr
        LEFT JOIN yearly_production prev
        ON CAST(curr.year AS INTEGER) = CAST(prev.year AS INTEGER) + 1
    )
    SELECT * FROM yoy_comparison
    ORDER BY year;
    """
    df4 = run_query(query4)
    if df4 is not None:
        print("Query 4: Year-over-year production variation:")
        print(df4.head(), "\n")

if __name__ == "__main__":
    main()
