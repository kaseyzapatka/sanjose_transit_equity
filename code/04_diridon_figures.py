# ==========================================================================
# 04_diridon_figures.py — Phase 3 static figures for the Diridon memo.
#
# Reads the pre-computed layers/tables from diridon_capacity.py and
# diridon_equity.py and renders three minimalist figures:
#   1. hero_map ............ station-area parcels by capacity, soft sites in
#                            coral, vulnerability tracts shaded plum underneath
#   2. capacity_by_zone .... gross vs soft-site capacity per zone
#   3. who_lives_here ...... station-area vs citywide dumbbell chart
#
# Outputs PNG (web) + PDF (print) to output/figures/.
#
# Author: Kasey Zapatka
# ==========================================================================

from pathlib import Path

import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.lines import Line2D
from shapely.geometry import Point

import viz_style as vs

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "output"
FIGS = OUTPUT / "figures"
MAPS = OUTPUT / "maps"
TABLES = OUTPUT / "tables"

DIRIDON_LONLAT = (-121.9036, 37.3292)
MILE_M = 1609.344
PLOT_CRS = 2227  # CA zone 3 (US ft) — correct local aspect


# ---------------------------------------------------------------------------
# 1. Hero map
# ---------------------------------------------------------------------------
def hero_map():
    parcels = gpd.read_parquet(MAPS / "diridon_station_capacity.geoparquet").to_crs(PLOT_CRS)
    tracts = gpd.read_parquet(MAPS / "diridon_station_tracts.geoparquet").to_crs(PLOT_CRS)

    pt = gpd.GeoSeries([Point(*DIRIDON_LONLAT)], crs=4326).to_crs(PLOT_CRS).iloc[0]
    ring = pt.buffer(MILE_M / 0.3048006096)  # mile -> US survey feet
    ring_gs = gpd.GeoSeries([ring], crs=PLOT_CRS)

    # clip tracts to the ring for a clean station-area frame
    tracts_clip = tracts.copy()
    tracts_clip["geometry"] = tracts_clip.intersection(ring)
    tracts_clip = tracts_clip[~tracts_clip.is_empty]

    fig, ax = plt.subplots(figsize=(9, 9))

    # vulnerability shading underneath (single plum ramp by score 0-4)
    for score, color in enumerate(vs.PLUM):
        sub = tracts_clip[tracts_clip["vulnerability_score"] == score]
        if len(sub):
            sub.plot(ax=ax, color=color, edgecolor="white", linewidth=0.6, zorder=1)
    tracts_clip.boundary.plot(ax=ax, color="white", linewidth=0.6, zorder=2)

    # target parcels: neutral base; developed vs soft
    target = parcels[parcels["is_target"]]
    built = target[~target["soft_site"]]
    soft = target[target["soft_site"]]
    built.plot(ax=ax, color=vs.NEUTRAL_D, edgecolor="none", alpha=0.85, zorder=3)
    soft.plot(ax=ax, color=vs.CORAL, edgecolor="none", zorder=4)

    # 1-mile ring + station point
    ring_gs.boundary.plot(ax=ax, color=vs.INK, linewidth=1.1, linestyle=(0, (5, 4)), zorder=5)
    ax.scatter([pt.x], [pt.y], s=90, color=vs.INK, marker="o", zorder=6,
               edgecolor=vs.PAPER, linewidth=1.2)
    ax.annotate("Diridon\nStation", (pt.x, pt.y), textcoords="offset points",
                xytext=(10, -4), fontsize=9.5, fontweight="bold", color=vs.INK, zorder=6)

    # frame
    minx, miny, maxx, maxy = ring_gs.total_bounds
    pad = (maxx - minx) * 0.04
    ax.set_xlim(minx - pad, maxx + pad)
    ax.set_ylim(miny - pad, maxy + pad)
    ax.set_aspect("equal")
    ax.axis("off")

    ax.set_title("Where San Jose can add homes near Diridon — and who lives there now",
                 fontsize=14, pad=14)

    legend = [
        Patch(facecolor=vs.CORAL, label="Soft site — underbuilt, high capacity"),
        Patch(facecolor=vs.NEUTRAL_D, label="Already-built capacity parcel"),
        Patch(facecolor=vs.PLUM[3], label="Higher-vulnerability tract"),
        Patch(facecolor=vs.PLUM[1], label="Lower-vulnerability tract"),
        Line2D([0], [0], color=vs.INK, lw=1.1, linestyle=(0, (5, 4)), label="1-mile radius"),
    ]
    ax.legend(handles=legend, loc="upper center", bbox_to_anchor=(0.5, -0.01),
              ncol=3, fontsize=8.5, frameon=False, handlelength=1.4,
              columnspacing=1.6, borderaxespad=0)

    fig.text(0.5, 0.025,
             "Capacity parcels = zones permitting housing (Downtown DC + UV/TR/UR/MUC/MUN); "
             "soft sites <15% lot coverage in OSM footprints.\n"
             "Sources: San Jose parcels & zoning; Title 20 / DSAP; ACS 2019–2023 5-yr; "
             "San Jose Equity Index; OpenStreetMap.",
             ha="center", va="top", fontsize=7.5, color=vs.MUTE)
    fig.subplots_adjust(bottom=0.13)
    return vs.save(fig, "hero_map", FIGS)


# ---------------------------------------------------------------------------
# 2. Capacity by zone (gross vs soft-site)
# ---------------------------------------------------------------------------
def capacity_by_zone():
    df = pd.read_csv(TABLES / "capacity_by_zone.csv")
    df = df.sort_values("gross_capacity")
    label = {"DC": "Downtown (DC)", "UV": "Urban Village", "TR": "Transit Residential",
             "UR": "Urban Residential", "MUC": "Mixed-Use Commercial",
             "MUN": "Mixed-Use Neighborhood"}
    df["name"] = df["zbase"].map(label).fillna(df["zbase"])
    y = np.arange(len(df))

    fig, ax = plt.subplots(figsize=(8.5, 4.6))
    ax.barh(y, df["gross_capacity"], color=vs.NEUTRAL, height=0.62,
            label="Gross zoned capacity", zorder=2)
    ax.barh(y, df["softsite_capacity"], color=vs.CORAL, height=0.62,
            label="Capacity on soft sites", zorder=3)
    ax.set_yticks(y)
    ax.set_yticklabels(df["name"])
    ax.set_xlabel("Theoretical dwelling-unit capacity")
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f"{int(x):,}"))

    for yi, g, s in zip(y, df["gross_capacity"], df["softsite_capacity"]):
        ax.text(g + max(df["gross_capacity"]) * 0.01, yi, f"{int(g):,}",
                va="center", fontsize=8.5, color=vs.MUTE)

    ax.set_title("Most capacity — and most soft-site potential — is in the Downtown core")
    ax.legend(loc="lower right", frameon=False, fontsize=9)
    fig.text(0.01, -0.02,
             "Densities: San Jose Title 20 Table 20-136 (UV/TR 250, UR 95, MUC 50, MUN 30 du/ac); "
             "Downtown 350 du/ac per the DSAP. Soft sites from OSM footprint coverage.",
             fontsize=7.5, color=vs.MUTE)
    return vs.save(fig, "capacity_by_zone", FIGS)


# ---------------------------------------------------------------------------
# 3. Who lives here — dumbbell (station vs citywide)
# ---------------------------------------------------------------------------
def who_lives_here():
    df = pd.read_csv(TABLES / "who_lives_here.csv")
    show = {
        "pct_renters": "Renters (% of households)",
        "rent_burdened_pct": "Rent-burdened renters (%)",
        "no_vehicle_pct": "Households without a vehicle (%)",
        "public_transit_pct": "Commute by transit (%)",
        "poverty_rate": "Poverty rate (%)",
    }
    df = df[df["indicator"].isin(show)].copy()
    df["name"] = df["indicator"].map(show)
    df = df.sort_values("station_area")
    y = np.arange(len(df))

    fig, ax = plt.subplots(figsize=(8.5, 4.4))
    ax.hlines(y, df["citywide"], df["station_area"], color=vs.NEUTRAL_D, lw=2.2, zorder=1)
    ax.scatter(df["citywide"], y, s=70, color=vs.MUTE, zorder=2, label="Citywide San Jose")
    ax.scatter(df["station_area"], y, s=85, color=vs.CORAL, zorder=3, label="Diridon 1-mile")
    ax.set_yticks(y)
    ax.set_yticklabels(df["name"])
    ax.set_xlabel("Percent")

    for yi, c, s in zip(y, df["citywide"], df["station_area"]):
        ax.text(s + 1.2, yi, f"{s:.0f}%", va="center", fontsize=8.5,
                color=vs.CORAL, fontweight="bold")
        ax.text(c - 1.2, yi, f"{c:.0f}%", va="center", ha="right", fontsize=8.5, color=vs.MUTE)

    ax.set_title("The station area is a renters' neighborhood, twice as transit-dependent")
    ax.legend(loc="lower right", frameon=False, fontsize=9)
    ax.margins(x=0.12)
    fig.text(0.01, -0.02,
             "Population-weighted tract averages within 1 mile vs all San Jose tracts. "
             "Source: ACS 2019–2023 5-year estimates.", fontsize=7.5, color=vs.MUTE)
    return vs.save(fig, "who_lives_here", FIGS)


def run():
    vs.apply_style()
    print("hero_map ->", hero_map())
    print("capacity_by_zone ->", capacity_by_zone())
    print("who_lives_here ->", who_lives_here())


if __name__ == "__main__":
    run()
