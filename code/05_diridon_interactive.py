# ==========================================================================
# diridon_interactive.py — Phase 3 interactive hero map (Leaflet via folium).
#
# Same story as the static hero map, but explorable: vulnerability tracts
# shaded underneath, capacity parcels on top (soft sites in coral), each parcel
# clickable for its zoning, capacity, footprint coverage, and the tract's
# renter / rent-burden / equity context.
#
# Output: output/maps/diridon_interactive_map.html  (self-contained)
#
# Author: Kasey Zapatka
# ==========================================================================

from pathlib import Path
import json

import geopandas as gpd
import folium
from folium.features import GeoJsonTooltip

import viz_style as vs

ROOT = Path(__file__).resolve().parents[1]
MAPS = ROOT / "output" / "maps"
DIRIDON_LONLAT = (-121.9036, 37.3292)
MILE_M = 1609.344


def build():
    parcels = gpd.read_parquet(MAPS / "diridon_station_capacity.geoparquet")
    tracts = gpd.read_parquet(MAPS / "diridon_station_tracts.geoparquet")

    lat, lon = DIRIDON_LONLAT[1], DIRIDON_LONLAT[0]
    m = folium.Map(location=[lat, lon], zoom_start=14, tiles="CartoDB positron")

    # --- vulnerability tracts underneath (plum ramp by score) ---
    def tract_style(feat):
        s = feat["properties"].get("vulnerability_score")
        color = vs.PLUM[int(s)] if s is not None else vs.PLUM[0]
        return {"fillColor": color, "color": "white", "weight": 0.8, "fillOpacity": 0.55}

    tr = tracts.copy()
    tr["renters_%"] = tr["pct_renters"].round(0)
    tr["rent_burdened_%"] = tr["rent_burdened_pct"].round(0)
    tr["no_vehicle_%"] = tr["no_vehicle_pct"].round(0)
    tcols = ["GEOID", "equity_score", "vulnerability_score",
             "renters_%", "rent_burdened_%", "no_vehicle_%"]
    folium.GeoJson(
        json.loads(tr[tcols + ["geometry"]].to_json()),
        style_function=tract_style,
        tooltip=GeoJsonTooltip(
            fields=tcols,
            aliases=["Tract:", "Equity Index (1-5):", "Vulnerability (0-4):",
                     "Renters:", "Rent-burdened:", "No vehicle:"],
            localize=True,
        ),
        name="Displacement vulnerability (tracts)",
    ).add_to(m)

    # --- 1-mile ring ---
    folium.Circle([lat, lon], radius=MILE_M, color=vs.INK, weight=2,
                  fill=False, dash_array="6,6").add_to(m)

    # --- capacity parcels (built grey, then soft coral on top) ---
    p = parcels.copy()
    p["capacity"] = p["gross_capacity"].round(0)
    p["coverage_%"] = (p["coverage"] * 100).round(0)
    pcols = ["PARCELID", "ZONING", "tier", "max_duac", "capacity", "coverage_%"]
    aliases = ["Parcel:", "Zoning:", "Tier:", "Max du/ac:",
               "Zoned capacity (units):", "Built coverage:"]

    built = p[~p["soft_site"]]
    soft = p[p["soft_site"]]

    folium.GeoJson(
        json.loads(built[pcols + ["geometry"]].to_json()),
        style_function=lambda x: {"fillColor": vs.NEUTRAL_D, "color": vs.NEUTRAL_D,
                                  "weight": 0.2, "fillOpacity": 0.7},
        tooltip=GeoJsonTooltip(fields=pcols, aliases=aliases, localize=True),
        name="Already-built capacity parcels",
    ).add_to(m)

    folium.GeoJson(
        json.loads(soft[pcols + ["geometry"]].to_json()),
        style_function=lambda x: {"fillColor": vs.CORAL, "color": vs.CORAL,
                                  "weight": 0.4, "fillOpacity": 0.85},
        tooltip=GeoJsonTooltip(fields=pcols, aliases=aliases, localize=True),
        name="Soft sites (underbuilt, high capacity)",
    ).add_to(m)

    folium.Marker([lat, lon], popup="Diridon Station",
                  icon=folium.Icon(color="black", icon="train", prefix="fa")).add_to(m)
    folium.LayerControl(collapsed=False).add_to(m)

    out = MAPS / "diridon_interactive_map.html"
    m.save(str(out))
    print("interactive map ->", out)
    return out


if __name__ == "__main__":
    build()
