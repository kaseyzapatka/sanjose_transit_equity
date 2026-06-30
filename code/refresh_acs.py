# ==========================================================================
# refresh_acs.py — refresh the tract ACS layer to a new 5-year vintage.
#
# Re-pulls ACS (keyed) and rebuilds san_jose_tracts_with_acs.geoparquet by
# merging the new indicators onto the EXISTING San Jose tract geometries. This
# avoids re-downloading TIGER and re-running the slow parcel join, and leaves
# the capacity layer (zoning-based) untouched — only the equity/demographic
# numbers change.
#
# Requires a Census API key (CENSUS_API_KEY env var or macOS Keychain item;
# read inside the process, never printed). Run:  python code/refresh_acs.py
# ==========================================================================

from pathlib import Path

import geopandas as gpd

from functions import pull_acs_data, compute_acs_indicators
from pipeline_utils import require, check_geo, save_parquet

YEAR = 2023  # ACS 2019-2023 5-year (year = end of the window)

ROOT = Path(__file__).resolve().parents[1]
TRACTS = ROOT / "data" / "processed" / "san_jose_tracts_with_acs.geoparquet"

REQUIRED = ["GEOID", "housing_units_occupied", "pct_renters", "rent_burdened_pct",
            "poverty_rate", "no_vehicle_pct", "public_transit_pct",
            "median_income", "median_rent"]


def run():
    print(f"Pulling ACS {YEAR-4}-{YEAR} 5-year (keyed)...")
    acs = compute_acs_indicators(pull_acs_data(state="CA", year=YEAR))
    missing = [c for c in REQUIRED if c not in acs.columns]
    require(not missing, f"ACS indicators missing columns: {missing}")

    print("Merging onto existing San Jose tract geometries...")
    existing = gpd.read_parquet(TRACTS)
    geo = existing[["GEOID", "geometry"]].drop_duplicates("GEOID")
    out = gpd.GeoDataFrame(geo.merge(acs, on="GEOID", how="inner"),
                           geometry="geometry", crs=existing.crs)
    check_geo(out, "refreshed San Jose tracts", min_rows=150)

    save_parquet(out, TRACTS)
    print(f"Refreshed {TRACTS.name}: {len(out)} tracts, ACS {YEAR-4}-{YEAR}.")
    print("Next: rerun 03_diridon_equity.py, 04_diridon_figures.py, "
          "05_diridon_interactive.py, then quarto render.")


if __name__ == "__main__":
    run()
