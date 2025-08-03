-- Table: production (oil and gas per state/month)
CREATE TABLE IF NOT EXISTS production (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE NOT NULL,
    state TEXT NOT NULL,
    oil_volume FLOAT,
    gas_volume FLOAT
);

-- Table: wells (metadata)
CREATE TABLE IF NOT EXISTS wells (
    id TEXT PRIMARY KEY, -- API_WellNo
    well_name TEXT,
    operator_name TEXT,
    status TEXT,
    county TEXT,
    latitude FLOAT,
    longitude FLOAT
);