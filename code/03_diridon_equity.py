# ==========================================================================
# 03_diridon_equity.py
#
# Phase 2 — Who lives in the Diridon station area now, and where new housing
# capacity overlaps displacement-vulnerable neighborhoods.
#
# Combines:
#   - San Jose Equity Index (official 1-5 priority score; higher = higher need)
#   - ACS 2022 5-yr tract indicators (renters, rent burden, poverty,
#     no-vehicle / transit dependence, income)
#   - The soft-site capacity layer from diridon_capacity.py
#
# Builds a transparent, flag-based displacement-vulnerability measure for the
# station-area tracts, characterizes the resident population, and quantifies
# how much net-new capacity sits in vulnerable / equity-priority tracts.
#
# Outputs (consumed by the Quarto memo + hero map):
#   output/tables/station_tract_profile.csv         (per-tract indicators + flags)
#   output/tables/who_lives_here.csv                (station-area summary vs citywide)
#   output/tables/capacity_x_vulnerability.csv      (net-new capacity by vulnerability)
#   output/maps/diridon_station_tracts.geoparquet   (tract layer for equity shading)
#
# Author: Kasey Zapatka
# ==========================================================================

from pathlib import Path
import warnings

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

warnings.filterwarnings("ignore", category=FutureWarning)

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
OUTPUT = ROOT / "output"

DIRIDON_LONLAT = (-121.9036, 37.3292)
MILE_M = 1609.344
METRIC_CRS = 3857

# Indicators used to characterize residents and flag vulnerability.
PROFILE_COLS = [
    "pct_renters", "rent_burdened_pct", "poverty_rate",
    "no_vehicle_pct", "public_transit_pct", "median_income", "median_rent",
]


# ---------------------------------------------------------------------------
# Step 1 — Tracts + Equity Index
# ---------------------------------------------------------------------------
def load_tracts_with_equity() -> gpd.GeoDataFrame:
    """San Jose ACS tracts joined to the official Equity Index score.

    Equity Index GEOID carries an ACS prefix; its FIPSCODE column matches the
    tract GEOID (11-digit). Higher EQUITYSCOR (1-5) = higher equity priority.
    """
    tracts = gpd.read_parquet(DATA / "processed" / "san_jose_tracts_with_acs.geoparquet")
    eq = gpd.read_parquet(DATA / "processed" / "equity.parquet")
    eq_lookup = (
        eq[["FIPSCODE", "EQUITYSCOR"]]
        .rename(columns={"FIPSCODE": "GEOID", "EQUITYSCOR": "equity_score"})
        .drop_duplicates("GEOID")
    )
    return tracts.merge(eq_lookup, on="GEOID", how="left")


# ---------------------------------------------------------------------------
# Step 2 — Station-area tracts + vulnerability flags
# ---------------------------------------------------------------------------
def flag_vulnerability(tracts: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Add displacement-vulnerability flags to every tract.

    Flags are defined relative to the CITYWIDE (San Jose) distribution so the
    cut-points are defensible rather than arbitrary:
      - renter_majority   : % renters above citywide median
      - rent_burdened     : % rent-burdened above citywide median
      - transit_dependent : % no-vehicle households above citywide median
      - equity_priority   : Equity Index score in top two categories (>= 4)
    vulnerability_score = sum of the four flags (0-4);
    high_vulnerability  = score >= 3.
    """
    s = tracts.copy()
    city_med = {c: s[c].median() for c in
                ["pct_renters", "rent_burdened_pct", "no_vehicle_pct"]}

    s["renter_majority"] = s["pct_renters"] > city_med["pct_renters"]
    s["rent_burdened"] = s["rent_burdened_pct"] > city_med["rent_burdened_pct"]
    s["transit_dependent"] = s["no_vehicle_pct"] > city_med["no_vehicle_pct"]
    s["equity_priority"] = s["equity_score"] >= 4

    flags = ["renter_majority", "rent_burdened", "transit_dependent", "equity_priority"]
    s["vulnerability_score"] = s[flags].sum(axis=1)
    s["high_vulnerability"] = s["vulnerability_score"] >= 3
    s.attrs["citywide_medians"] = city_med
    return s


def select_station(tracts: gpd.GeoDataFrame, miles: float = 1.0) -> gpd.GeoDataFrame:
    """Tracts intersecting the `miles`-mile station buffer."""
    pt = gpd.GeoSeries([Point(*DIRIDON_LONLAT)], crs=4326).to_crs(METRIC_CRS).iloc[0]
    buf = pt.buffer(miles * MILE_M)
    sel_idx = tracts.to_crs(METRIC_CRS).geometry.intersects(buf)
    return tracts[sel_idx].copy()


# ---------------------------------------------------------------------------
# Step 3 — Who lives here (station area vs citywide)
# ---------------------------------------------------------------------------
def who_lives_here(station_tracts: gpd.GeoDataFrame,
                   all_tracts: gpd.GeoDataFrame) -> pd.DataFrame:
    """Population-weighted station-area averages vs citywide, for the memo."""
    def wmean(df, col, w="housing_units_occupied"):
        d = df[[col, w]].dropna()
        return np.average(d[col], weights=d[w]) if len(d) and d[w].sum() else np.nan

    rows = []
    for col in PROFILE_COLS:
        rows.append({
            "indicator": col,
            "station_area": round(wmean(station_tracts, col), 1),
            "citywide": round(wmean(all_tracts, col), 1),
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Step 4 — Net-new capacity x vulnerability
# ---------------------------------------------------------------------------
def capacity_by_vulnerability(flagged_tracts: gpd.GeoDataFrame) -> pd.DataFrame:
    """Assign each capacity parcel to its tract and total capacity by the
    tract's vulnerability score. Answers: how much soft-site capacity sits in
    high-vulnerability / equity-priority neighborhoods?

    Parcels are merged against ALL flagged tracts (by GEOID) so capacity is
    not dropped for parcels whose tract lies just outside the buffer ring.
    """
    parcels = gpd.read_parquet(OUTPUT / "maps" / "diridon_station_capacity.geoparquet")

    # A handful of large downtown parcels have no tract GEOID from the pipeline;
    # assign them spatially (parcel centroid within tract) so their capacity is
    # not dropped from the equity overlay.
    missing = parcels["GEOID"].isna()
    if missing.any():
        cent = parcels.loc[missing].copy()
        cent["geometry"] = cent.geometry.centroid
        joined = gpd.sjoin(
            cent[["geometry"]], flagged_tracts[["GEOID", "geometry"]],
            how="left", predicate="within",
        )
        parcels.loc[missing, "GEOID"] = joined["GEOID"].values

    cols = ["GEOID", "vulnerability_score", "high_vulnerability",
            "equity_priority", "equity_score"]
    p = parcels.merge(flagged_tracts[cols], on="GEOID", how="left")

    by = p.groupby("vulnerability_score").apply(lambda g: pd.Series({
        "target_parcels": len(g),
        "gross_capacity": round(g["gross_capacity"].sum()),
        "softsite_capacity": round(g["softsite_capacity"].sum()),
    })).reset_index()

    total_soft = p["softsite_capacity"].sum()
    high_soft = p.loc[p["high_vulnerability"] == True, "softsite_capacity"].sum()
    eqp_soft = p.loc[p["equity_priority"] == True, "softsite_capacity"].sum()
    by.attrs["share_high_vuln"] = round(100 * high_soft / total_soft, 1) if total_soft else 0
    by.attrs["share_equity_priority"] = round(100 * eqp_soft / total_soft, 1) if total_soft else 0
    return by


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
def run(miles: float = 1.0) -> gpd.GeoDataFrame:
    tables = OUTPUT / "tables"
    maps = OUTPUT / "maps"
    tables.mkdir(parents=True, exist_ok=True)

    print("Loading tracts + Equity Index...")
    tracts = load_tracts_with_equity()

    print("Flagging displacement vulnerability (citywide-relative)...")
    flagged = flag_vulnerability(tracts)

    print(f"Selecting station-area tracts ({miles}-mile)...")
    station = select_station(flagged, miles=miles)
    print(f"  {len(station)} tracts | high-vulnerability: {int(station['high_vulnerability'].sum())}")

    # per-tract profile
    prof_cols = (["GEOID", "equity_score"] + PROFILE_COLS +
                 ["renter_majority", "rent_burdened", "transit_dependent",
                  "equity_priority", "vulnerability_score", "high_vulnerability"])
    profile = station[prof_cols].sort_values("vulnerability_score", ascending=False)
    profile.round(1).to_csv(tables / "station_tract_profile.csv", index=False)

    # who lives here
    wlh = who_lives_here(station, tracts)
    wlh.to_csv(tables / "who_lives_here.csv", index=False)
    print("\n=== Who lives in the station area (pop-weighted) vs citywide ===")
    print(wlh.to_string(index=False))

    # capacity x vulnerability (merge against all flagged tracts)
    cxv = capacity_by_vulnerability(flagged)
    cxv.to_csv(tables / "capacity_x_vulnerability.csv", index=False)
    print("\n=== Net-new soft-site capacity by tract vulnerability score ===")
    print(cxv.to_string(index=False))
    print(f"\nShare of net-new capacity in HIGH-vulnerability tracts: "
          f"{cxv.attrs['share_high_vuln']}%")
    print(f"Share in equity-priority tracts (score >= 4): "
          f"{cxv.attrs['share_equity_priority']}%")

    # tract layer for hero-map shading
    station.to_crs(4326).to_parquet(maps / "diridon_station_tracts.geoparquet")
    print("\nWrote tables to output/tables/ and tract layer to "
          "output/maps/diridon_station_tracts.geoparquet")
    return station


if __name__ == "__main__":
    run()
