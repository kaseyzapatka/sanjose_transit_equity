"""
03_build_pipeline.py
-------------------------------------------------------------
Build the parcel-level dataset for ML suitability modeling.
Includes:
- Loading raw/processed parcel data
- Spatial joins with schools, TAZs, and stations
- Feature engineering
- Target variable creation
- Saves processed dataset for ML analysis
-------------------------------------------------------------
"""

# --------------------------
# LIBRARIES
# --------------------------
# file path
import os
from pathlib import Path

# data management
import pandas as pd
import geopandas as gpd
import numpy as np
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

# seetings
pd.set_option('display.max_columns', None)  # Show all columns when printing the DataFrame


# --------------------------
# FILE PATHS
# --------------------------

#
# Set folder file paths
# ----------------------------------------

PROCESSED_DIR = Path("data/processed")
RAW_DIR = Path("data/raw")

#
# Identify specific files
# ----------------------------------------
PARCEL_FILE = PROCESSED_DIR / "parcels_with_zoning.parquet"
PARCEL_FILE = PROCESSED_DIR / "parcels_with_zoning_and_tract_data.parquet"
ACS_FILE = PROCESSED_DIR / "san_jose_tracts_with_acs.geoparquet"
TIP_ZONES = RAW_DIR / "Multimodal_Transportation_Improvement_Plan_Areas.geojson"


# --------------------------
# FUNCTIONS
# --------------------------

#
# Diridon station
# ----------------------------------------
def load_diridon_station():
    """
    Load Diridon Station location as a GeoDataFrame.
    Returns a GeoDataFrame with one point geometry.
    """
    # Approximate coordinates for Diridon Station, San Jose
    lat, lon = 37.329, -121.901
    gdf = gpd.GeoDataFrame(
        [{'name': 'Diridon Station'}],
        geometry=[Point(lon, lat)],
        crs='EPSG:4326'  # WGS84 Latitude/Longitude
    )
    return gdf

#
# Compute distance to Diridon Station
# ----------------------------------------
def compute_distance_to_station(parcels_gdf, station_gdf=None, units='meters'):
    """
    Compute distance from each parcel to Diridon Station.
    
    Args:
        parcels_gdf: GeoDataFrame with parcel geometries
        station_gdf: GeoDataFrame with station geometry (optional)
        units: 'meters' or 'miles' (default: 'meters')
    
    Returns:
        GeoDataFrame with 'dist_to_station_meters' or 'dist_to_station_miles' column.
    """
    if station_gdf is None:
        station_gdf = load_diridon_station()
    
    parcels = parcels_gdf.copy()
    
    # Project to meters
    parcels_proj = parcels.to_crs(epsg=3857)
    station_proj = station_gdf.to_crs(epsg=3857)
    
    # Get station point
    station_point = station_proj.geometry.iloc[0]
    
    # Compute distance in meters
    distance_meters = parcels_proj.geometry.distance(station_point)
    
    # Create column with appropriate name and units
    if units == 'miles':
        parcels_proj['dist_to_station_miles'] = distance_meters / 1609.34
    else:
        parcels_proj['dist_to_station_meters'] = distance_meters
    
    # Project back to EPSG:4326
    parcels_final = parcels_proj.to_crs(epsg=4326)
    
    return parcels_final

#
# Create binary features
# ----------------------------------------
def zoning_binary_features(parcels_gdf, zoning_types):
    """
    Create binary features for given zoning types.
    
    Args:
        parcels_gdf: GeoDataFrame with a 'zoning' column
        zoning_types: list of zoning categories to create binary columns for
        
    Returns:
        GeoDataFrame with new binary columns for each zoning type
    """
    for z_type in zoning_types:
        col_name = f'is_{z_type}'
        parcels_gdf[col_name] = parcels_gdf['zoning'].apply(lambda x: 1 if x == z_type else 0)
    return parcels_gdf


#
# Spatially join transit to parcel data to determine if parcel is in Transit Improvement Plan Areas
# ----------------------------------------
def parcel_in_tip(parcels_gdf, tip_gdf, tip_name='TIP'):
    """
    Create a binary feature indicating whether each parcel is within a TIP polygon.
    
    Args:
        parcels_gdf: GeoDataFrame with parcel geometries (points or polygons)
        tip_gdf: GeoDataFrame with TIP polygons
        tip_name: name for the binary column to create (default 'TIP')
    
    Returns:
        parcels_gdf with a new column: 1 if inside a TIP polygon, 0 otherwise
    """
    # Make copies to avoid modifying original data
    parcels = parcels_gdf.copy()
    tip_proj = tip_gdf.to_crs(parcels.crs).copy()
    
    # Reset index to ensure uniqueness
    parcels = parcels.reset_index(drop=True)
    tip_proj = tip_proj.reset_index(drop=True)
    
    # Perform spatial join
    joined = gpd.sjoin(parcels, tip_proj[['geometry']], how='left', predicate='intersects')
    
    # Handle duplicate indices from multiple intersections by grouping by the original parcel index and check if ANY intersection exists
    in_tip = joined.groupby(joined.index)['index_right'].apply(lambda x: x.notnull().any()).astype(int)
    
    # Map back to parcels (this handles the duplicate index issue)
    parcels[tip_name] = parcels.index.map(in_tip).fillna(0).astype(int)
    
    return parcels


# --------------------------
# LOAD DATA
# --------------------------
print("Loading parcel data...")
parcels = gpd.read_parquet(PARCEL_FILE).to_crs(epsg=3857)

print("Loading districts/zones...")
tip = gpd.read_file(TIP_ZONES).to_crs(epsg=3857)

print("Loading Diridon Station...")
station = load_diridon_station().to_crs(epsg=3857)

# --------------------------
# PROCESSING
# --------------------------

#
# Spatial join
# ----------------------------------------
print("Creating spatial flags for parcels...")
# Use the parcel_in_tip function to create a binary column
parcels = parcel_in_tip(parcels, tip_gdf=tip, tip_name='in_taz')

#
# Target zoning flag
# ----------------------------------------
URBAN_ZONING = ['Transit Residential', 'Urban Residential',
                'Mixed Use Commercial', 'Urban Village',
                'Municipal/Neighborhood Mixed Use', 'Urban Village Commercial']
parcels["urban_zone_flag"] = parcels["zoning"].isin(URBAN_ZONING).astype(int)


#
# Feature engineering
# ----------------------------------------
print("Engineering features...")
# compute distance of each parcel to Diridon station
parcels = compute_distance_to_station(parcels, station, units = "miles")
#parcels = zoning_binary_features(parcels, zoning_types="zoning")
parcels["parcel_area"] = parcels.geometry.area



# Example ACS variables
acs_vars = [
    'public_transit_pct', 
    'walked_pct', 
    'drove_pct', 
    'pct_renters',
    'vacancy_rate',
    "median_income",
    "median_rent",
    "pct_white",
    "pct_black",
    "pct_asian",
    "pct_latino",
    "pct_college_plus"
]

# Save final dataset for analysis
feature_cols = [
    "dist_to_station_miles",
    "parcel_area",
    "in_taz",
] + acs_vars + [col for col in parcels.columns if col.startswith("zone_")]


parcels_model = parcels.dropna(subset=feature_cols)

output_file = PROCESSED_DIR / "parcels_for_ml.parquet"
parcels_model.to_parquet(output_file)

print(f"\n✓ Pipeline complete! Processed dataset saved to: {output_file}")

