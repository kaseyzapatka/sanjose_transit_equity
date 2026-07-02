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

    # vulnerability shading underneath — three classes matching the memo's
    # language: Lower (0-1 flags), Moderate (2), Higher (3+ of 5)
    vclass = {"lower": (vs.PLUM[1], tracts_clip["vulnerability_score"] <= 1),
              "moderate": (vs.PLUM[2], tracts_clip["vulnerability_score"] == 2),
              "higher": (vs.PLUM[3], tracts_clip["vulnerability_score"] >= 3)}
    for color, mask in vclass.values():
        sub = tracts_clip[mask]
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
        Patch(facecolor=vs.PLUM[2], label="Moderate-vulnerability tract"),
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
# 3. Capacity vs plan — today's stock, the DSAP program, the zoning envelope
# ---------------------------------------------------------------------------
def capacity_vs_plan():
    b = pd.read_csv(TABLES / "benchmarks.csv").set_index("benchmark")["homes"]
    rows = [
        ("Homes today (est.)", b["existing_units_est"], vs.NEUTRAL_D),
        ("New homes City plans call for", b["planned_homes_in_ring"], vs.PLUM[3]),
        ("Zoning envelope (max buildout)", b["zoning_envelope"], vs.CORAL),
    ]
    y = np.arange(len(rows))[::-1]

    fig, ax = plt.subplots(figsize=(8.5, 3.2))
    envelope = b["zoning_envelope"]
    for yi, (label, val, color) in zip(y, rows):
        ax.barh(yi, val, color=color, height=0.58, zorder=2)
        emphasize = val == envelope
        ax.text(val + envelope * 0.012, yi, f"~{round(val, -2):,.0f}",
                va="center", fontsize=12 if emphasize else 10,
                fontweight="heavy" if emphasize else "normal", color=vs.INK)
    ax.set_yticks(y)
    ax.set_yticklabels([r[0] for r in rows])
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f"{int(x):,}"))
    ax.margins(x=0.14)

    ratio = b["zoning_envelope"] / b["planned_homes_in_ring"]
    ax.set_title(f"Zoning allows about {ratio:.1f}× more homes than City plans call for")
    fig.text(0.01, -0.04,
             "Homes today: ACS 2019–2023 housing units, apportioned to the 1-mile ring. "
             "Planned homes: SJ Growth Areas 2040 programs apportioned to the ring\n"
             "(amended DSAP program substituted for its polygon). Envelope: this analysis "
             "(Title 20 / DSAP maximum densities); a ceiling, not a forecast.",
             fontsize=7.5, color=vs.MUTE)
    return vs.save(fig, "capacity_vs_plan", FIGS)


# ---------------------------------------------------------------------------
# 4. Who lives here — dumbbell (station vs citywide) + income panel
# ---------------------------------------------------------------------------
def who_lives_here():
    df = pd.read_csv(TABLES / "who_lives_here.csv").set_index("indicator")
    show = {
        "pct_renters": "Renters (% of households)",
        "rent_burdened_pct": "Rent-burdened renters (%)",
        "no_vehicle_pct": "Households without a vehicle (%)",
        "public_transit_pct": "Commute by transit (%)",
        "poverty_rate": "Poverty rate (%)",
    }
    pct = df.loc[list(show)].rename(index=show).sort_values("station_area")
    y = np.arange(len(pct))

    # two stacked panels: percent indicators on top, income strip below
    fig, (ax, ax2) = plt.subplots(
        2, 1, figsize=(8.5, 5.6),
        gridspec_kw={"height_ratios": [4.2, 1], "hspace": 0.45})

    # top panel — percent indicators
    ax.hlines(y, pct["citywide"], pct["station_area"], color=vs.NEUTRAL_D, lw=2.2, zorder=1)
    ax.scatter(pct["citywide"], y, s=70, color=vs.MUTE, zorder=2, label="Citywide San Jose")
    ax.scatter(pct["station_area"], y, s=85, color=vs.CORAL, zorder=3, label="Diridon 1-mile")
    ax.set_yticks(y)
    ax.set_yticklabels(pct.index)
    ax.set_xlabel("Percent")
    for yi, c, s in zip(y, pct["citywide"], pct["station_area"]):
        ax.text(s + 1.2, yi, f"{s:.0f}%", va="center", fontsize=8.5,
                color=vs.CORAL, fontweight="bold")
        ax.text(c - 1.2, yi, f"{c:.0f}%", va="center", ha="right", fontsize=8.5, color=vs.MUTE)
    ax.legend(loc="lower right", frameon=False, fontsize=9)
    ax.margins(x=0.14)
    ax.set_title("The station area: more renters, more transit-dependent, lower incomes",
                 fontsize=13, pad=12)

    # bottom strip — median household income (same dumbbell language, $ scale)
    inc = df.loc["median_income"]
    ax2.hlines(0, inc["citywide"] / 1e3, inc["station_area"] / 1e3,
               color=vs.NEUTRAL_D, lw=2.2, zorder=1)
    ax2.scatter(inc["citywide"] / 1e3, 0, s=70, color=vs.MUTE, zorder=2)
    ax2.scatter(inc["station_area"] / 1e3, 0, s=85, color=vs.CORAL, zorder=3)
    ax2.text(inc["station_area"] / 1e3, 0.22, f"${inc['station_area']/1e3:,.0f}k",
             ha="center", va="bottom", fontsize=8.5, color=vs.CORAL, fontweight="bold")
    ax2.text(inc["citywide"] / 1e3, 0.22, f"${inc['citywide']/1e3:,.0f}k",
             ha="center", va="bottom", fontsize=8.5, color=vs.MUTE)
    ax2.set_yticks([0])
    ax2.set_yticklabels(["Median household income"])
    ax2.set_ylim(-0.6, 0.9)
    ax2.set_xlim(0, 160)  # zero baseline so the income gap reads at true scale
    ax2.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f"${int(x)}k"))
    ax2.set_xlabel("Median household income (thousands of dollars)")

    fig.text(0.01, -0.02,
             "Tract averages weighted by occupied housing units, within 1 mile vs all San Jose tracts. "
             "Source: ACS 2019–2023 5-year estimates.", fontsize=7.5, color=vs.MUTE)
    return vs.save(fig, "who_lives_here", FIGS)


def run():
    vs.apply_style()
    print("hero_map ->", hero_map())
    print("capacity_by_zone ->", capacity_by_zone())
    print("capacity_vs_plan ->", capacity_vs_plan())
    print("who_lives_here ->", who_lives_here())


if __name__ == "__main__":
    run()
