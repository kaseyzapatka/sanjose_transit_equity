# ==========================================================================
# check_outputs.py — one-command smoke test for the Diridon pipeline.
#
# Verifies that a clean environment can reproduce the headline spatial facts
# and that the committed output tables are internally consistent. Exits non-zero
# on the first failure so it is usable in CI or a pre-commit check.
#
#   python code/check_outputs.py
# ==========================================================================

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data" / "processed"
TABLES = ROOT / "output" / "tables"
MAPS = ROOT / "output" / "maps"

DIRIDON_LONLAT = (-121.9036, 37.3292)
STATE_PLANE = 2227
MILE_FT = 1609.344 / 0.3048006096012192   # 1 mile in US survey feet

# Expected values (order-of-magnitude, with tolerance) for the true 1-mile area.
EXPECT = {
    "station_parcels": (5279, 60),
    "target_parcels": (1199, 30),
    "gross_capacity": (119703, 1500),
    "softsite_capacity": (42520, 1000),
    "station_tracts": (13, 1),
}

_failures = []


def check(name, ok, detail=""):
    flag = "PASS" if ok else "FAIL"
    print(f"  [{flag}] {name}{(' — ' + detail) if detail else ''}")
    if not ok:
        _failures.append(name)


def near(value, key):
    target, tol = EXPECT[key]
    return abs(value - target) <= tol


def main():
    print("Diridon pipeline smoke test\n")

    # 1) source data loads, correct CRS, finite bounds
    parcels = gpd.read_parquet(DATA / "parcels_with_zoning_and_tract_data.parquet")
    check("source parcels CRS is EPSG:2227", parcels.crs and parcels.crs.to_epsg() == STATE_PLANE,
          f"got {parcels.crs.to_epsg() if parcels.crs else None}")
    check("source parcel bounds finite", bool(np.all(np.isfinite(parcels.total_bounds))))

    # 2) 1-mile selection reproduces expected counts (State Plane buffer)
    pt = gpd.GeoSeries([Point(*DIRIDON_LONLAT)], crs=4326).to_crs(STATE_PLANE).iloc[0]
    buf = pt.buffer(MILE_FT)
    within = parcels[parcels.geometry.centroid.within(buf)]
    check("1-mile parcel count", near(len(within), "station_parcels"), f"{len(within)}")

    # 3) committed output tables are consistent with expectations
    head = pd.read_csv(TABLES / "capacity_headline.csv").iloc[0]
    check("target parcels", near(head["target_parcels"], "target_parcels"), f"{head['target_parcels']}")
    check("gross capacity", near(head["gross_capacity"], "gross_capacity"), f"{head['gross_capacity']:,}")
    check("soft-site capacity", near(head["softsite_capacity"], "softsite_capacity"), f"{head['softsite_capacity']:,}")
    check("strict floor < soft-site total",
          head["softsite_capacity_strict"] < head["softsite_capacity"],
          f"{head['softsite_capacity_strict']:,} < {head['softsite_capacity']:,}")

    # 4) by-tier table sums to the headline gross
    tier = pd.read_csv(TABLES / "capacity_by_tier.csv")
    check("tier totals sum to headline gross",
          abs(tier["gross_capacity"].sum() - head["gross_capacity"]) <= 2)

    # 5) capacity + tract layers present, in EPSG:4326, non-empty
    cap = gpd.read_parquet(MAPS / "diridon_station_capacity.geoparquet")
    tr = gpd.read_parquet(MAPS / "diridon_station_tracts.geoparquet")
    check("capacity layer non-empty", len(cap) > 0, f"{len(cap)} parcels")
    check("station tracts count", near(len(tr), "station_tracts"), f"{len(tr)}")
    # A few edge parcels (centroid just outside any San Jose tract) carry no
    # GEOID in the capacity layer; the equity step assigns these spatially.
    check("GEOID coverage in capacity layer (remainder patched in equity step)",
          cap["GEOID"].notna().mean() >= 0.98, f"{cap['GEOID'].notna().mean():.1%} populated")

    print()
    if _failures:
        print(f"FAILED ({len(_failures)}): {', '.join(_failures)}")
        sys.exit(1)
    print("All checks passed.")


if __name__ == "__main__":
    main()
