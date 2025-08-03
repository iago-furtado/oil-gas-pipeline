# Oil Gas Pipeline
ETL Project - Alberta Oil and Gas Wells Data

Overview
--------
This project performs an ETL (Extract, Transform, Load) pipeline to process and analyze well location data. The project uses Python, pandas, and geopandas to clean the data, engineer features, and perform geospatial aggregation.

Setup Instructions
------------------
1. Clone the repository.
2. Create a virtual environment (optional but recommended):
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
3. Install dependencies:
   pip install -r requirements.txt

How to Run the ETL Script
-------------------------
Execute the ETL pipeline by running:
   python etl_pipeline.py

Outputs Location
----------------
Processed outputs are saved in the `data/processed/` directory:
- `cleaned_wells.csv`: Cleaned well location data.
- `enhanced_wells.csv`: Data after feature engineering.
- `wells_by_county.csv`: Aggregated count of wells per county.
- `wells.geojson`: [Optional] GeoJSON file for use in mapping tools.

Assumptions Made
----------------
- The input data is stored in `data/raw/wells.csv`.
- County names are consistently labeled for aggregation.
- Only basic data cleaning was performed (removing missing coordinates and duplicates).
- Coordinate Reference System (CRS) is assumed to be consistent across shapefiles and point geometries.