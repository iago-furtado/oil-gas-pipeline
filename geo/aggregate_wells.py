import os
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

# Paths
PROCESSED_DATA_PATH = "data/processed/"
RAW_DATA_PATH = "data/raw/"

def aggregate_wells_per_county():
    # Load cleaned wells data 
    wells_df = pd.read_csv(os.path.join(PROCESSED_DATA_PATH, "wells_clean.csv"))

    # Aggregate wells count per county
    wells_by_county = wells_df.groupby("county").size().reset_index(name="well_count")

    # Save CSV
    wells_by_county.to_csv(os.path.join(PROCESSED_DATA_PATH, "wells_by_county.csv"), index=False)

    print("✅ wells_by_county.csv saved")

    return wells_by_county

def produce_geojson():
    wells_df = pd.read_csv(os.path.join(PROCESSED_DATA_PATH, "wells_clean.csv"))

    # Create geometry points from longitude and latitude
    geometry = [Point(xy) for xy in zip(wells_df["longitude"], wells_df["latitude"])]
    gdf = gpd.GeoDataFrame(wells_df, geometry=geometry, crs="EPSG:4326")

    # Save GeoJSON
    geojson_path = os.path.join(PROCESSED_DATA_PATH, "wells.geojson")
    gdf.to_file(geojson_path, driver='GeoJSON')

    print("✅ wells.geojson saved")

if __name__ == "__main__":
    aggregate_wells_per_county()
    produce_geojson()  
