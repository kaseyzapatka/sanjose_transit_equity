# ==========================================================================
# 01_data_pipeline.py
#
# Summary:
# A minimal ETL pipeline that:
#   • extracts spatial datasets,
#   • joins parcels to zoning districts,
#   • deduplicates parcels,
#   • saves outputs to GeoJSON and Parquet.
#
# This script uses helper functions stored in functions.py.
# ==========================================================================

import os

from functions import (
    # functions to load data
    load_parcels,
    load_zoning,
    #load_railroad,
    #load_bikeways,
    #load_bike_racks,
    load_affordable_housing,
    load_equity_index,
    # functions for spatial join
    join_parcels_zoning,
    attach_tract_data_to_parcels,
    # functions for saving
    save_parquet,
    # functions for census calculations
    pull_acs_data,
    compute_acs_indicators,
    pull_tracts,
    pull_places,
    subset_city_tracts,
    merge_tracts_with_acs, 
    # functions for zoning
    abbreviate_zoning,
    zoning_classification, 
    classify_zoning
)

#
# Settings
# ----------------------------------------
OUTPUT_DIR = "../output"
DATA_DIR = "../data/"
os.makedirs(OUTPUT_DIR, exist_ok=True)


#
# Define pipeline function
# ----------------------------------------
def run_etl():
    print("Starting ETL pipeline...\n")

    # ======================
    #       EXTRACT
    # ======================
    
    #
    # Apply classification dictionary
    # ----------------------------------------
    print("Extracting data...")
    
    # extract San Jose spatial data
    parcels = load_parcels()
    zoning = load_zoning()
    #railroad = load_railroad()
    #bikeways = load_bikeways()
    #bikeracks = load_bike_racks()
    affordable = load_affordable_housing()
    equity = load_equity_index()

    # extract census data
    acs_raw = pull_acs_data(state="CA", year=2022)
    tracts = pull_tracts(state="CA", year=2022)
    places = pull_places(state="CA", year=2022)

    # ======================
    #      TRANSFORM
    # ======================
    
    #
    # Process parcel data
    # ----------------------------------------
    print("Processing parcels data...")
    # spatially join zoning to parcels data
    parcels_zoned = join_parcels_zoning(parcels, zoning)

    #
    # Process zoning data
    # ----------------------------------------
    print("Processing zoning data...")
    # rewrite zoning abbreviations to be clearer for analysis
    parcels_zoned["zoning"] = parcels_zoned["ZONING"].apply(abbreviate_zoning)
    # create a new zoning classification variable in parcels data
    parcels_zoned["zoning_class"] = parcels_zoned["ZONING"].apply(classify_zoning)
    # create indicator of planned zoning
    parcels_zoned["zoning_planned"] = parcels_zoned["ZONING"].str.upper().str.contains(r"\(PD\)", na=False)
    

    #
    # Process Census data
    # ----------------------------------------
    # 1. Create percent variables 
    print("1. Processing Census data...")
    acs = compute_acs_indicators(acs_raw)

    # 2. Subset tracts to just San Jose
    print("2. Subsetting tracts to San Jose...")
    san_jose_tracts = subset_city_tracts(tracts, places, city_name="San Jose")

    # 3. Merge tracts to just San Jose
    print("3. Merging ACS with tract geometries...")
    sj_acs = merge_tracts_with_acs(san_jose_tracts, acs)


    #
    # Attach tract-level ACS data to parcels
    # ----------------------------------------
    print("Attaching tract-level ACS data to parcels...")
    tract_fields = [
        "vacancy_rate",
        "median_rent",
        "pct_white",
        "pct_black",
        "pct_asian",
        "pct_latino",
        "pct_college_plus"
    ]
    parcels_with_tract_data = attach_tract_data_to_parcels(parcels_zoned, sj_acs, tract_fields=None) # change this
    
    
    # ======================
    #        LOAD
    # ======================
    print("Saving necessary outputs...")
    save_parquet(parcels_zoned, path=f"{DATA_DIR}/processed/parcels_with_zoning.parquet")    
    save_parquet(zoning, path=f"{DATA_DIR}/processed/zoning.parquet")
    save_parquet(equity, path=f"{DATA_DIR}/processed/equity.parquet")
    save_parquet(affordable, path=f"{DATA_DIR}/processed/affordable.parquet")
    save_parquet(sj_acs, path=f"{DATA_DIR}/processed/san_jose_tracts_with_acs.geoparquet")
    save_parquet(parcels_with_tract_data, path=f"{DATA_DIR}/processed/parcels_with_zoning_and_tract_data.parquet")


# run pipeline
if __name__ == "__main__":
    run_etl()
