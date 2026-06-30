# ==========================================================================
# 02_diridon_capacity.py
#
# Phase 1 — Parcel-level housing capacity within 1 mile of Diridon Station.
#
# Replaces the original placeholder density analysis with:
#   1. REAL maximum densities from San Jose Municipal Code Title 20
#      (Ch. 20.55, Table 20-136) plus the Downtown / Diridon Station Area
#      Plan (DSAP) designation for Downtown (DC) parcels.
#   2. Capacity restricted to the 1-mile Diridon station area.
#   3. Soft-site capacity: a soft site is a high-capacity parcel that
#      is currently vacant or barely built. Because Santa Clara County does
#      not publish parcel-level assessed values (improvement-to-land ratio is
#      a paid bulk-data product), underutilization is measured from OPEN
#      building footprints (OpenStreetMap): the share of each lot covered by
#      a building. Low coverage -> soft site.
#
# Outputs (consumed by the Quarto memo):
#   output/tables/capacity_by_tier.csv
#   output/tables/capacity_by_zone.csv
#   output/tables/softsite_threshold_sensitivity.csv
#   output/tables/capacity_headline.csv
#   output/maps/diridon_station_capacity.geoparquet   (per-parcel layer for hero map)
#
# Author: Kasey Zapatka
# ==========================================================================

from pathlib import Path
import json
import warnings

import numpy as np
import pandas as pd
import geopandas as gpd
import requests
from shapely.geometry import Point, Polygon

from pipeline_utils import require, check_geo, save_csv, save_parquet

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", message=".*centroid.*")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
OUTPUT = ROOT / "output"

DIRIDON_LONLAT = (-121.9036, 37.3292)   # Diridon Station (lon, lat)
MILE_M = 1609.344                        # 1 mile in meters
PARCEL_CRS = 2227                        # NAD83 / California zone 3 (US ft) — data's native CRS
SQFT_PER_ACRE = 43_560
# Distance work is done in EPSG:2227 (California State Plane, US survey feet),
# which is locally accurate. Buffering in EPSG:3857 (Web Mercator) would shrink
# the radius by ~1/cos(lat) ≈ 0.8x at Diridon's latitude — i.e. a "1-mile"
# Web-Mercator buffer is really ~0.8 mile on the ground.
MILE_FT = MILE_M / 0.3048006096012192    # 1 mile in US survey feet (≈ 5279.99)

# Maximum dwelling units / acre for 100% residential projects.
#   Mixed-use / urban-village districts: San Jose Municipal Code Title 20,
#   Ch. 20.55 "Urban Village and Mixed Use Zoning Districts," Table 20-136.
#     UV / TR ......... 55-250 and 50-250 du/ac -> max 250
#     UR .............. 30-95 du/ac          -> max 95
#     MUC ............. max 50 du/ac
#     MUN ............. max 30 du/ac
#     UVC ............. residential not permitted -> 0
MAX_DUAC_TITLE20 = {"UV": 250, "TR": 250, "UR": 95, "MUC": 50, "MUN": 30, "UVC": 0}

#   Downtown (DC): density is governed by the General Plan "Downtown" land use
#   designation and the Diridon Station Area Plan (DSAP, amended 2021), not by a
#   Title 20 du/ac figure. The Downtown designation allows up to 350 du/ac.
DC_DUAC_DSAP = 350

# Soft-site definition: a target-zone parcel whose building footprint covers
# less than this share of the lot is treated as vacant / surface-parking /
# underbuilt -> a soft site.
SOFT_COVERAGE_THRESHOLD = 0.15


# ---------------------------------------------------------------------------
# Step 1 — Station-area parcels (fixes the empty diridon_parcels_1mile.parquet)
# ---------------------------------------------------------------------------
def load_station_area(miles: float = 1.0) -> gpd.GeoDataFrame:
    """Return parcels whose centroid falls within `miles` of Diridon Station.

    The buffer is built in EPSG:3857 (meters) for an accurate distance; the
    returned frame is in the source parcel CRS (EPSG:2227, US survey feet) so
    that SHAPE_Area-derived acreage stays consistent.
    """
    parcels = gpd.read_parquet(
        DATA / "processed" / "parcels_with_zoning_and_tract_data.parquet"
    )
    check_geo(parcels, "processed parcels (source)", min_rows=100_000)

    # Buffer in the data's native State Plane CRS (US survey feet) — locally
    # accurate, and avoids the Web Mercator distortion / bulk reprojection that
    # otherwise shrinks the radius and can fail on some PROJ installs.
    pt = gpd.GeoSeries([Point(*DIRIDON_LONLAT)], crs=4326).to_crs(PARCEL_CRS).iloc[0]
    buffer = pt.buffer(miles * MILE_FT)
    station = parcels[parcels.geometry.centroid.within(buffer)].copy()
    check_geo(station, f"{miles}-mile station parcels", min_rows=500)

    # base zoning code, stripped of (PD)/(CL) overlays
    station["zbase"] = (
        station["ZONING"].str.replace(r"\(.*\)", "", regex=True).str.strip()
    )
    station["lot_sqft"] = station["SHAPE_Area_left"]
    station["acres"] = station["lot_sqft"] / SQFT_PER_ACRE
    return station


# ---------------------------------------------------------------------------
# Step 2 — Open building footprints (OpenStreetMap, cached)
# ---------------------------------------------------------------------------
def load_footprints(refresh: bool = False) -> gpd.GeoDataFrame:
    """Load OSM building footprints for the station-area bounding box.

    Cached to data/raw/osm_buildings_diridon.json so the analysis is
    reproducible without re-querying the Overpass API. `building:levels` is
    parsed where present (used for a rough built-FAR; ~3% of buildings carry
    it, so footprint coverage is the primary signal).
    """
    cache = DATA / "raw" / "osm_buildings_diridon.json"
    if refresh or not cache.exists():
        _fetch_overpass(cache)

    raw = json.load(open(cache))
    geoms, levels = [], []

    def parse_levels(tags):
        v = tags.get("building:levels")
        try:
            return float(str(v).split(";")[0].split("-")[0])
        except (TypeError, ValueError):
            return np.nan

    def add_ring(coords, tags):
        if len(coords) < 4:
            return
        poly = Polygon([(p["lon"], p["lat"]) for p in coords])
        if poly.is_valid and poly.area > 0:
            geoms.append(poly)
            levels.append(parse_levels(tags))

    for el in raw["elements"]:
        tags = el.get("tags", {})
        if el["type"] == "way" and "geometry" in el:
            add_ring(el["geometry"], tags)
        elif el["type"] == "relation":
            for m in el.get("members", []):
                if m.get("role") == "outer" and "geometry" in m:
                    add_ring(m["geometry"], tags)

    fp = gpd.GeoDataFrame({"levels": levels}, geometry=geoms, crs=4326).to_crs(PARCEL_CRS)
    return fp


def _fetch_overpass(cache: Path) -> None:
    """Query the Overpass API for buildings in the 1-mile bbox and cache JSON."""
    lat, lon = DIRIDON_LONLAT[1], DIRIDON_LONLAT[0]
    dlat = 1.05 * MILE_M / 111_320
    dlon = 1.05 * MILE_M / (111_320 * np.cos(np.radians(lat)))
    s, n, w, e = lat - dlat, lat + dlat, lon - dlon, lon + dlon
    query = (
        f"[out:json][timeout:180];"
        f'(way["building"]({s},{w},{n},{e});'
        f' relation["building"]({s},{w},{n},{e}););'
        f"out geom;"
    )
    resp = requests.post(
        "https://overpass-api.de/api/interpreter",
        data={"data": query},
        headers={"User-Agent": "diridon-capacity-study/1.0 (research)"},
        timeout=240,
    )
    resp.raise_for_status()
    cache.parent.mkdir(parents=True, exist_ok=True)
    json.dump(resp.json(), open(cache, "w"))


# ---------------------------------------------------------------------------
# Step 3 — Footprint coverage per parcel
# ---------------------------------------------------------------------------
def add_footprint_coverage(parcels: gpd.GeoDataFrame,
                           footprints: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Attach built-coverage and rough built-FAR to each parcel.

    Footprints are clipped to parcel boundaries (overlay intersection) so a
    building spanning a lot line is not double-counted.
    """
    inter = gpd.overlay(
        footprints[["geometry", "levels"]],
        parcels[["PARCELID", "geometry"]],
        how="intersection",
        keep_geom_type=True,
    )
    inter["fp_sqft"] = inter.geometry.area
    inter["floor_sqft"] = inter["fp_sqft"] * inter["levels"].fillna(1)
    agg = inter.groupby("PARCELID").agg(
        fp_sqft=("fp_sqft", "sum"),
        floor_sqft=("floor_sqft", "sum"),
        bldg_count=("fp_sqft", "size"),
    ).reset_index()

    out = parcels.merge(agg, on="PARCELID", how="left")
    for col in ["fp_sqft", "floor_sqft", "bldg_count"]:
        out[col] = out[col].fillna(0)
    out["coverage"] = (out["fp_sqft"] / out["lot_sqft"]).clip(0, 1)
    out["built_far"] = out["floor_sqft"] / out["lot_sqft"]
    return out


# ---------------------------------------------------------------------------
# Step 4 — Capacity + soft sites
# ---------------------------------------------------------------------------
def compute_capacity(parcels: gpd.GeoDataFrame,
                     threshold: float = SOFT_COVERAGE_THRESHOLD) -> gpd.GeoDataFrame:
    """Assign max du/ac, gross capacity, tier, and soft-site flag."""
    p = parcels.copy()
    p["max_duac"] = p["zbase"].map(MAX_DUAC_TITLE20)
    p.loc[p["zbase"] == "DC", "max_duac"] = DC_DUAC_DSAP

    p["is_target"] = p["max_duac"].notna() & (p["max_duac"] > 0)
    p["gross_capacity"] = np.where(p["is_target"], p["acres"] * p["max_duac"], 0.0)
    p["tier"] = np.select(
        [p["zbase"] == "DC", p["is_target"] & (p["zbase"] != "DC")],
        ["Downtown (DC)", "Mixed-use (UV/TR/UR/MUC/MUN)"],
        default="Other / non-residential",
    )
    p["soft_site"] = p["is_target"] & (p["coverage"] < threshold)
    p["softsite_capacity"] = np.where(p["soft_site"], p["gross_capacity"], 0.0)
    return p


# ---------------------------------------------------------------------------
# Step 5 — Summaries + export
# ---------------------------------------------------------------------------
def summarize_and_export(parcels: gpd.GeoDataFrame) -> dict:
    tables = OUTPUT / "tables"
    maps = OUTPUT / "maps"
    tables.mkdir(parents=True, exist_ok=True)
    maps.mkdir(parents=True, exist_ok=True)

    target = parcels[parcels["is_target"]].copy()
    # fail fast before writing: a zero-target selection means a CRS/zoning
    # problem upstream; don't overwrite good outputs with empties.
    require(len(target) >= 100,
            f"target (housing-permitting) parcels = {len(target)} (expected ~750); aborting")

    # by tier
    by_tier = target.groupby("tier").apply(lambda g: pd.Series({
        "parcels": len(g),
        "acres": round(g["acres"].sum(), 1),
        "gross_capacity": round(g["gross_capacity"].sum()),
        "soft_parcels": int(g["soft_site"].sum()),
        "soft_acres": round(g.loc[g["soft_site"], "acres"].sum(), 1),
        "softsite_capacity": round(g["softsite_capacity"].sum()),
        "median_coverage_pct": round(g["coverage"].median() * 100),
    })).reset_index()
    save_csv(by_tier, tables / "capacity_by_tier.csv")

    # by zone (mixed-use detail + DC)
    order = ["UV", "TR", "UR", "MUC", "MUN", "DC"]
    by_zone = target.groupby("zbase").apply(lambda g: pd.Series({
        "parcels": len(g),
        "acres": round(g["acres"].sum(), 1),
        "max_duac": int(g["max_duac"].iloc[0]),
        "gross_capacity": round(g["gross_capacity"].sum()),
        "soft_parcels": int(g["soft_site"].sum()),
        "softsite_capacity": round(g["softsite_capacity"].sum()),
    })).reindex([z for z in order if z in target["zbase"].unique()]).reset_index()
    save_csv(by_zone, tables / "capacity_by_zone.csv")

    # threshold sensitivity
    rows = []
    for thr in [0.05, 0.10, 0.15, 0.20, 0.25]:
        soft = target[target["coverage"] < thr]
        rows.append({
            "coverage_threshold": thr,
            "soft_parcels": len(soft),
            "softsite_capacity": round(soft["gross_capacity"].sum()),
            "dc_softsite_capacity": round(
                soft.loc[soft["zbase"] == "DC", "gross_capacity"].sum()),
            "mixeduse_softsite_capacity": round(
                soft.loc[soft["zbase"] != "DC", "gross_capacity"].sum()),
        })
    save_csv(pd.DataFrame(rows), tables / "softsite_threshold_sensitivity.csv")

    # developability sensitivity: parcels with ZERO detected footprint may be
    # genuine vacant lots / surface parking OR non-developable civic, rail, or
    # station land carrying DC zoning. The "strict" floor drops all zero-coverage
    # parcels — conservative (it also drops some real vacant land), but it bounds
    # how much of the soft-site number rests on unverified zero-coverage parcels.
    soft = target[target["soft_site"]]
    zero_cov = soft[soft["coverage"] == 0]
    strict = soft[soft["coverage"] > 0]
    dev = pd.DataFrame([
        {"basis": "soft sites (<15% coverage)", "parcels": len(soft),
         "capacity": round(soft["gross_capacity"].sum())},
        {"basis": "  of which zero-coverage", "parcels": len(zero_cov),
         "capacity": round(zero_cov["gross_capacity"].sum())},
        {"basis": "strict floor (0% < coverage < 15%)", "parcels": len(strict),
         "capacity": round(strict["gross_capacity"].sum())},
    ])
    save_csv(dev, tables / "softsite_developability_sensitivity.csv")

    # headline numbers
    headline = pd.DataFrame([{
        "station_parcels_1mi": len(parcels),
        "target_parcels": len(target),
        "target_acres": round(target["acres"].sum(), 1),
        "gross_capacity": round(target["gross_capacity"].sum()),
        "softsite_capacity": round(target["softsite_capacity"].sum()),
        "softsite_capacity_strict": round(strict["gross_capacity"].sum()),
        "soft_parcels": int(target["soft_site"].sum()),
        "soft_parcels_zero_coverage": int(len(zero_cov)),
        "soft_coverage_threshold": SOFT_COVERAGE_THRESHOLD,
    }])
    save_csv(headline, tables / "capacity_headline.csv")

    # per-parcel layer for the hero map (EPSG:4326 for web mapping)
    keep = ["PARCELID", "APN", "ZONING", "zbase", "GEOID", "acres", "lot_sqft",
            "coverage", "built_far", "bldg_count", "max_duac", "tier",
            "is_target", "gross_capacity", "soft_site", "softsite_capacity",
            "geometry"]
    layer = parcels[parcels["is_target"]][keep].to_crs(4326)
    save_parquet(layer, maps / "diridon_station_capacity.geoparquet")

    return {"by_tier": by_tier, "by_zone": by_zone, "headline": headline}


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
def run(refresh_footprints: bool = False) -> gpd.GeoDataFrame:
    print("Loading 1-mile station-area parcels...")
    station = load_station_area(miles=1.0)
    print(f"  {len(station):,} parcels within 1 mile")

    print("Loading OSM building footprints...")
    fp = load_footprints(refresh=refresh_footprints)
    print(f"  {len(fp):,} footprints")

    print("Computing footprint coverage...")
    station = add_footprint_coverage(station, fp)

    print("Computing capacity + soft sites...")
    station = compute_capacity(station)

    res = summarize_and_export(station)

    print("\n=== Capacity by tier (1-mile, real Title 20 / DSAP densities) ===")
    print(res["by_tier"].to_string(index=False))
    print("\n=== Capacity by zone ===")
    print(res["by_zone"].to_string(index=False))
    h = res["headline"].iloc[0]
    print(f"\nTOTAL gross zoned capacity: {h['gross_capacity']:,.0f}")
    print(f"TOTAL soft-site capacity (gross capacity on underbuilt parcels): "
          f"{h['softsite_capacity']:,.0f} ({h['soft_parcels']} soft-site parcels)")
    print("\nWrote tables to output/tables/ and parcel layer to "
          "output/maps/diridon_station_capacity.geoparquet")
    return station


if __name__ == "__main__":
    run()
