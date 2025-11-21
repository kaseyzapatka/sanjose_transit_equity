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
    load_railroad,
    load_bikeways,
    load_bike_racks,
    load_affordable_housing,
    load_equity_index,
    # functions for spatial join
    join_parcels_zoning,
    # functions for saving
    save_parquet,
    # functions for census calculations
    pull_acs_data,
    compute_acs_indicators,
    pull_tracts,
    pull_places,
    subset_city_tracts,
    merge_tracts_with_acs
)

OUTPUT_DIR = "../output"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def run_etl():
    print("Starting ETL pipeline...\n")

    # ======================
    #       EXTRACT
    # ======================
    print("Extracting data...")
    parcels = load_parcels()
    zoning = load_zoning()
    railroad = load_railroad()
    bikeways = load_bikeways()
    bikeracks = load_bike_racks()
    affordable = load_affordable_housing()
    equity = load_equity_index()

    # ======================
    #      TRANSFORM
    # ======================
    print("Joining parcels to zoning...")
    parcels_zoned = join_parcels_zoning(parcels, zoning)

    # ======================
    #        LOAD
    # ======================
    print("Saving outputs...")
    save_parquet(parcels_zoned, path=f"{OUTPUT_DIR}/parcels_with_zoning.parquet")    
    save_parquet(zoning, path=f"{OUTPUT_DIR}/zoning.parquet")
    save_parquet(equity, path=f"{OUTPUT_DIR}/equity.parquet")
    save_parquet(affordable, path=f"{OUTPUT_DIR}/affordable.parquet")

    # ======================================================
    #   ACS + TRACT GEOMETRY PIPELINE FOR SAN JOSE
    # ======================================================
    print("\nExtracting ACS and tract data...")

    # Extract
    acs_raw = pull_acs_data(state="CA", year=2022)
    tracts = pull_tracts(state="CA", year=2022)
    places = pull_places(state="CA", year=2022)

    # Transform
    print("Processing ACS indicators...")
    acs = compute_acs_indicators(acs_raw)

    print("Subsetting tracts to San Jose...")
    san_jose_tracts = subset_city_tracts(tracts, places, city_name="San Jose")

    print("Merging ACS with tract geometries...")
    sj_acs = merge_tracts_with_acs(san_jose_tracts, acs)

    # Load
    print("Saving ACS outputs...")
    save_parquet(sj_acs, path=f"{OUTPUT_DIR}/san_jose_tracts_with_acs.geoparquet")


    print("\nETL run complete.")


if __name__ == "__main__":
    run_etl()
