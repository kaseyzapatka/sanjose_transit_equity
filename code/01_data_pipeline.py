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
    load_parcels,
    load_zoning,
    load_railroad,
    load_bikeways,
    load_bike_racks,
    load_affordable_housing,
    load_equity_index,
    join_parcels_zoning,
    save_parquet
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



    print("\nETL run complete.")


if __name__ == "__main__":
    run_etl()
