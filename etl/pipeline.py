import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# Load environment variables
load_dotenv("config/settings.env")
DB_URL = os.getenv("DB_URL")

# Paths
RAW_DATA_PATH = "data/raw/"
PROCESSED_DATA_PATH = "data/processed/"

# Create SQLAlchemy engine
engine = create_engine(DB_URL)

def load_and_clean_production():
    # Read crude oil CSV (skip first 4 lines of metadata)
    crude_path = os.path.join(RAW_DATA_PATH, "U.S._crude_oil_production.csv")
    crude_df = pd.read_csv(crude_path, skiprows=4)

    # Read natural gas CSV (skip first 4 lines)
    gas_path = os.path.join(RAW_DATA_PATH, "U.S._natural_gas_production.csv")
    gas_df = pd.read_csv(gas_path, skiprows=4)

    # Rename columns for clarity
    crude_df.columns = ['Month', 'WV_Oil', 'PA_Oil']
    gas_df.columns = ['Month', 'WV_Gas', 'PA_Gas']

    # Convert Month to datetime (parse format like 'May 2025')
    crude_df['Month'] = pd.to_datetime(crude_df['Month'], format='%b %Y')
    gas_df['Month'] = pd.to_datetime(gas_df['Month'], format='%b %Y')

    # Merge on Month
    production_df = pd.merge(crude_df, gas_df, on='Month', how='inner')

    # Reshape to long format by state (WV, PA)
    # Oil volumes
    oil = production_df.melt(id_vars=['Month'], value_vars=['WV_Oil', 'PA_Oil'], 
                             var_name='state_oil', value_name='oil_volume')
    oil['state'] = oil['state_oil'].str.extract(r'^(WV|PA)_')[0].map({'WV':'West Virginia','PA':'Pennsylvania'})
    oil = oil.drop(columns='state_oil')

    # Gas volumes
    gas = production_df.melt(id_vars=['Month'], value_vars=['WV_Gas', 'PA_Gas'], 
                             var_name='state_gas', value_name='gas_volume')
    gas['state'] = gas['state_gas'].str.extract(r'^(WV|PA)_')[0].map({'WV':'West Virginia','PA':'Pennsylvania'})
    gas = gas.drop(columns='state_gas')

    # Merge oil and gas on Month and state
    df = pd.merge(oil, gas, on=['Month','state'])

    # Rename columns to match DB schema
    df = df.rename(columns={'Month': 'date', 'state': 'state'})

    # Save cleaned production data CSV
    os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)
    df.to_csv(os.path.join(PROCESSED_DATA_PATH, 'production_clean.csv'), index=False)

    return df


def load_and_clean_wells():
    wells_path = os.path.join(RAW_DATA_PATH, "wellspublic.csv")
    wells_df = pd.read_csv(wells_path)

    # Select relevant columns & rename
    wells_df = wells_df.rename(columns={
        "API_WellNo": "id",
        "Well_Name": "well_name",
        "Company_name": "operator_name",
        "Well_Status": "status",
        "County": "county",
        "Surface_latitude": "latitude",
        "Surface_Longitude": "longitude"
    })

    wells_df = wells_df[["id", "well_name", "operator_name", "status", "county", "latitude", "longitude"]]

    # Drop rows with missing or invalid coordinates (latitude/longitude)
    wells_df = wells_df.dropna(subset=['latitude', 'longitude'])

    # Filter out invalid coordinates (optional: latitude between -90 and 90, longitude between -180 and 180)
    wells_df = wells_df[(wells_df['latitude'].between(-90,90)) & (wells_df['longitude'].between(-180,180))]

    # Save cleaned wells data CSV
    wells_df.to_csv(os.path.join(PROCESSED_DATA_PATH, 'wells_clean.csv'), index=False)

    return wells_df


def load_to_db(df, table_name):
    try:
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print(f"Data loaded into '{table_name}' table.")
    except SQLAlchemyError as e:
        print(f"Failed to load data into {table_name}: {e}")


def main():
    production_df = load_and_clean_production()
    load_to_db(production_df, 'production')

    wells_df = load_and_clean_wells()
    load_to_db(wells_df, 'wells')

if __name__ == "__main__":
    main()