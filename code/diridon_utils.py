"""
diridon_utils.py

Utility functions for Deliverable #2:
Diridon Station Opportunity Analysis.

All logic is contained here. The accompanying
Jupyter notebook calls these functions in order.

Author: Kasey Zapatka
"""

from pathlib import Path
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import folium
from folium.features import GeoJsonTooltip

# ---------------------------------------------
# Configuration
# ---------------------------------------------

DIRIDON_LON_LAT = (-121.9036, 37.3292)
MILE_IN_METERS = 1609.344


# ---------------------------------------------
# Step 1: Load Data
# ---------------------------------------------

def load_data(parcels_path="../data/processed/parcels_with_zoning.parquet", 
              tracts_path="../data/processed/san_jose_tracts_with_acs.geoparquet",
              parcels_tracts_path="../data/processed/parcels_with_zoning_and_tract_data.parquet"):
    parcels = gpd.read_parquet(parcels_path)
    tracts = gpd.read_parquet(tracts_path)
    parcels_tracts = gpd.read_parquet(parcels_tracts_path)
    return parcels, tracts , parcels_tracts


# ---------------------------------------------
# Step 2: Reproject
# ---------------------------------------------

def reproject_for_buffering(parcels, tracts):
    """
    Reproject parcels and tracts to EPSG:3857 for accurate distance calculations.
    """
    target_crs = "EPSG:3857"
    return parcels.to_crs(target_crs), tracts.to_crs(target_crs)


# ---------------------------------------------
# Step 3: Build Diridon Buffers
# ---------------------------------------------

def build_diridon_buffers():
    """
    Create 1-mile and 2-mile buffers around Diridon Station.
    Returns buffers in EPSG:3857 (meters) for analysis.
    """
    pt = gpd.GeoSeries([Point(*DIRIDON_LON_LAT)], crs="EPSG:4326")
    pt_m = pt.to_crs("EPSG:3857").iloc[0]
    buffer_1m = pt_m.buffer(1 * MILE_IN_METERS)
    buffer_2m = pt_m.buffer(2 * MILE_IN_METERS)
    return pt_m, buffer_1m, buffer_2m


# ---------------------------------------------
# Step 4: Parcel Summary
# ---------------------------------------------

def summarize_parcels(parcels_m, buffer_1m):
    """
    Summarize parcels within 1-mile buffer.
    
    Parameters:
    -----------
    parcels_m : GeoDataFrame in EPSG:3857
    buffer_1m : Polygon geometry in EPSG:3857
    """
    parcels_m = parcels_m.copy()
    parcels_m["centroid"] = parcels_m.geometry.centroid
    centroids = parcels_m.set_geometry("centroid")

    within_1m = centroids[centroids.centroid.within(buffer_1m)].copy()
    within_1m = within_1m.set_geometry("geometry")  # Reset to original geometry
    
    uv = within_1m[within_1m["zoning_class"] == "Urban Village"]

    summary = {
        "total_parcels": len(within_1m),
        "uv_parcels": len(uv)
    }

    return within_1m, uv, summary


# ---------------------------------------------
# Step 5: ACS Summary
# ---------------------------------------------

def summarize_acs(tracts_m, buffer_2m, acs_cols):
    """
    Summarize ACS data for tracts within 2-mile buffer.
    
    Parameters:
    -----------
    tracts_m : GeoDataFrame in EPSG:3857
    buffer_2m : Polygon geometry in EPSG:3857
    acs_cols : dict mapping labels to column names
    """
    tracts_m = tracts_m.copy()
    tracts_m["in_buffer"] = tracts_m.geometry.intersects(buffer_2m)
    tracts_sel = tracts_m[tracts_m["in_buffer"]].copy()

    summary = {}
    for label, col in acs_cols.items():
        if col in tracts_sel.columns:
            summary[label] = tracts_sel[col].mean()

    return tracts_sel, summary


# ---------------------------------------------
# Step 6: Maps (EPSG:4326 + EPSG:3857 for buffers)
# ---------------------------------------------

def create_maps(parcels, tracts, output_dir):
    """
    Create static map of parcels and zoning near Diridon Station
    with accurate 1-mile and 2-mile buffers.

    ALL DATA IS PLOTTED IN EPSG:4326 (to match folium),
    but buffers are created in EPSG:3857 to ensure accurate distance.

    Parameters:
    -----------
    parcels : GeoDataFrame
    tracts : GeoDataFrame
    output_dir : str or Path

    Returns:
    --------
    Path : path to saved PNG map file
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # ---------------------------------------------------------
    # 1. Reproject parcels + tracts to EPSG:4326 for consistent plotting
    # ---------------------------------------------------------
    parcels_4326 = parcels.to_crs(4326)
    tracts_4326 = tracts.to_crs(4326)

    # ---------------------------------------------------------
    # 2. Create Diridon Station point (4326 -> 3857)
    # ---------------------------------------------------------
    diridon_station_4326 = gpd.GeoDataFrame(
        geometry=[Point(*DIRIDON_LON_LAT)],
        crs="EPSG:4326"
    )

    # Convert to EPSG:3857 for accurate buffer geometry
    diridon_station_3857 = diridon_station_4326.to_crs(3857)

    # ---------------------------------------------------------
    # 3. Create 1-mile and 2-mile buffers in meters (EPSG:3857)
    # ---------------------------------------------------------
    buffer_1mile_3857 = diridon_station_3857.buffer(MILE_IN_METERS)
    buffer_2mile_3857 = diridon_station_3857.buffer(2 * MILE_IN_METERS)

    # Convert buffers back to 4326 for overlay + plotting
    buffer_1mile_4326 = gpd.GeoDataFrame(geometry=buffer_1mile_3857, crs=3857).to_crs(4326)
    buffer_2mile_4326 = gpd.GeoDataFrame(geometry=buffer_2mile_3857, crs=3857).to_crs(4326)

    # ---------------------------------------------------------
    # 4. Subset parcels within buffers (now everything matches in 4326)
    # ---------------------------------------------------------
    parcels_within_2mile = gpd.overlay(
        parcels_4326,
        buffer_2mile_4326,
        how="intersection"
    )

    parcels_within_1mile = gpd.overlay(
        parcels_4326,
        buffer_1mile_4326,
        how="intersection"
    )

    # ---------------------------------------------------------
    # 5. Identify urban-zoned parcels + define colors
    # ---------------------------------------------------------
    urban_zoning = [
        "Urban Village", "Urban Village Commercial", "Urban Residential",
        "Transit Residential", "Mixed Use", "Mixed Use Commercial",
        "Municipal/Neighborhood Mixed Use"
    ]

    uz_within_1mile = parcels_within_1mile[
        parcels_within_1mile["zoning"].isin(urban_zoning)
    ].copy()

    zoning_colors = {
        "Urban Village": "#e41a1c",
        "Urban Village Commercial": "#377eb8",
        "Urban Residential": "#4daf4a",
        "Transit Residential": "#984ea3",
        "Mixed Use": "#ff7f00",
        "Mixed Use Commercial": "#ffff33",
        "Municipal/Neighborhood Mixed Use": "#a65628"
    }

    # ---------------------------------------------------------
    # 6. Build the figure
    # ---------------------------------------------------------
    fig, ax = plt.subplots(figsize=(14, 12))

    # Tracts background
    tracts_4326.plot(ax=ax, color="lightgrey", edgecolor="grey", alpha=0.3)

    # 2-mile buffer
    buffer_2mile_4326.boundary.plot(
        ax=ax, color="grey", linestyle="--", linewidth=0.5, label="2-mile radius"
    )

    # Base parcels (within 2 miles)
    parcels_within_2mile.plot(
        ax=ax, color="lightgrey", edgecolor="white", linewidth=0.1
    )

    # Urban zoning colors
    for zone_type in urban_zoning:
        zp = uz_within_1mile[uz_within_1mile["zoning"] == zone_type]
        if len(zp) > 0:
            zp.plot(
                ax=ax,
                color=zoning_colors[zone_type],
                alpha=0.7,
                edgecolor="black",
                linewidth=0.2,
                label=f"{zone_type} ({len(zp)} parcels)"
            )

    # 1-mile buffer
    buffer_1mile_4326.boundary.plot(
        ax=ax, color="black", linestyle="--", linewidth=1, label="1-mile radius"
    )

    # Station point
    diridon_station_4326.plot(
        ax=ax,
        color="black",
        marker=".",
        markersize=200,
        label="Diridon Station",
        zorder=10
    )

    # ---------------------------------------------------------
    # Set extent to 2-mile buffer
    # ---------------------------------------------------------
    xmin, ymin, xmax, ymax = buffer_2mile_4326.total_bounds
    pad = 0.01  # small lat/lon padding
    ax.set_xlim(xmin - pad, xmax + pad)
    ax.set_ylim(ymin - pad, ymax + pad)

    # ---------------------------------------------------------
    # Title + legend
    # ---------------------------------------------------------
    ax.set_title(
        "Urban-zoned parcels within 1 mile of San Jose Diridon Station",
        fontsize=14, fontweight="bold", pad=20
    )

    # Custom legend
    legend_handles = [
        Patch(facecolor=zoning_colors[z], edgecolor="black", label=z)
        for z in uz_within_1mile["zoning"].unique()
    ]
    legend_handles.extend([
        Patch(facecolor="none", edgecolor="black", linestyle="--", label="1-mile radius"),
        Patch(facecolor="none", edgecolor="grey", linestyle="--", label="2-mile radius"),
    ])
    legend_handles.append(
        plt.Line2D([0], [0], marker=".", color="black", markersize=12,
                   linestyle="None", label="Diridon Station")
    )

    ax.legend(handles=legend_handles, loc="upper right", fontsize=9, framealpha=0.95)
    ax.axis("off")

    # ---------------------------------------------------------
    # Footnote with parcel counts
    # ---------------------------------------------------------
    total = len(parcels_within_1mile)
    footnote_lines = [
        "Share of urban-zoned parcels within 1 mile of San Jose Diridon Station:"
    ]
    for z in urban_zoning:
        count = len(uz_within_1mile[uz_within_1mile["zoning"] == z])
        if count > 0:
            pct = (count / total) * 100
            footnote_lines.append(f"  {z}: {pct:.1f}% ({count:,} parcels)")
    footnote_lines.append(f"Total parcels: {total:,}")

    fig.text(0.12, 0.02, "\n".join(footnote_lines), fontsize=8, ha="left")

    # ---------------------------------------------------------
    # Save
    # ---------------------------------------------------------
    outpath =  Path(output_dir) / "diridon_buffer_map.pdf"
    plt.savefig(outpath, dpi=150, bbox_inches="tight")
    plt.close()

    print(f"✓ Map saved to: {outpath}")
    print("✓ Zoning breakdown:")
    for z in urban_zoning:
        c = len(uz_within_1mile[uz_within_1mile["zoning"] == z])
        if c > 0:
            print(f"  - {z}: {c} parcels")

    return outpath

# ---------------------------------------------
# Step 7: Interactive Map
# ---------------------------------------------
import folium
from folium.features import GeoJsonTooltip
from pathlib import Path
import json

def create_interactive_map(parcels_with_tract_data, tracts, output_dir):
    """
    Create an interactive map of parcels near Diridon Station, showing zoning and
    tract-level ACS variables for each parcel, including parcel ID and tract ID.

    Parameters
    ----------
    parcels_with_tract_data : GeoDataFrame
        Parcel geometries with zoning info and attached tract-level ACS variables.
    tracts : GeoDataFrame
        Census tract geometries.
    output_dir : str or Path
        Directory to save the HTML map.

    Returns
    -------
    Path
        Path to saved HTML map file.
    """

    # Reproject to EPSG:4326 (required by Folium)
    parcels_proj = parcels_with_tract_data.to_crs(epsg=4326)
    tracts_proj = tracts.to_crs(epsg=4326)

    # Diridon Station coordinates
    diridon_station_coords = DIRIDON_LON_LAT  # (lon, lat)

    # Create base map centered on Diridon Station
    m = folium.Map(
        location=[diridon_station_coords[1], diridon_station_coords[0]],
        zoom_start=14,
        tiles="CartoDB positron"
    )

    # ------------------------
    # Add tracts as background
    # ------------------------
    tracts_clean = tracts_proj[["geometry"]].copy()  # only geometry column
    folium.GeoJson(
        json.loads(tracts_clean.to_json()),
        style_function=lambda x: {
            "fillColor": "lightblue",
            "color": "grey",
            "weight": 0.5,
            "fillOpacity": 0.2,
        },
        name="Census Tracts"
    ).add_to(m)

    # ------------------------
    # Add 1-mile and 2-mile buffers
    # ------------------------
    folium.Circle(
        location=[diridon_station_coords[1], diridon_station_coords[0]],
        radius=MILE_IN_METERS,
        color="black",
        weight=2,
        fill=False,
        popup="1-mile radius"
    ).add_to(m)

    folium.Circle(
        location=[diridon_station_coords[1], diridon_station_coords[0]],
        radius=2 * MILE_IN_METERS,
        color="grey",
        weight=1,
        fill=False,
        popup="2-mile radius"
    ).add_to(m)

    # ------------------------
    # Urban zoning parcels with tract data in tooltip
    # ------------------------
    zoning_colors = {
        "Urban Village": "#e41a1c",
        "Urban Village Commercial": "#377eb8",
        "Urban Residential": "#4daf4a",
        "Transit Residential": "#984ea3",
        "Mixed Use": "#ff7f00",
        "Mixed Use Commercial": "#ffff33",
        "Municipal/Neighborhood Mixed Use": "#a65628"
    }

    urban_zoning = list(zoning_colors.keys())
    parcels_uv = parcels_proj[parcels_proj["zoning"].isin(urban_zoning)].copy()

    # Fields from tract-level data to show in popup
    tract_fields = [
        "public_transit_pct",
        "walked_pct",
        "drove_pct",
        "pct_renters",
        "vacancy_rate",
        "median_rent",
        "median_income",
        "pct_white",
        "pct_black",
        "pct_asian",
        "pct_latino",
        "pct_college_plus"
    ]

    # Add parcels by zoning type
    for zone_type, color in zoning_colors.items():
        subset = parcels_uv[parcels_uv["zoning"] == zone_type]
        if not subset.empty:
            # Keep only serializable columns
            subset_clean = subset[
                ["geometry", "PARCELID", "GEOID", "zoning"] + 
                [f for f in tract_fields if f in subset.columns]
            ].copy()
            
            # Tooltip fields: zoning + ACS tract fields + parcel + tract ID
            # Columns to show in tooltip (only include columns that exist in subset_clean)
            tooltip_fields = [col for col in ["PARCELID", "GEOID", "zoning"] + tract_fields if col in subset_clean.columns]

            # Aliases must match exactly
            aliases = []
            for col in tooltip_fields:
                if col == "PARCELID":
                    aliases.append("Parcel ID:")
                elif col == "GEOID":
                    aliases.append("Tract GEOID:")
                elif col == "zoning":
                    aliases.append("Zoning type:")
                else:
                    aliases.append(col.replace("_", " ").title())

            folium.GeoJson(
                json.loads(subset_clean.to_json()),
                style_function=lambda x, c=color: {
                    "fillColor": c,
                    "color": "black",
                    "weight": 0.3,
                    "fillOpacity": 0.7
                },
                tooltip=GeoJsonTooltip(
                    fields=tooltip_fields,
                    aliases=aliases,
                    localize=True
                ),
                name=f"{zone_type} ({len(subset_clean)} parcels)"
            ).add_to(m)

    # ------------------------
    # Add Diridon Station marker
    # ------------------------
    folium.Marker(
        location=[diridon_station_coords[1], diridon_station_coords[0]],
        popup="Diridon Station",
        icon=folium.Icon(color="black", icon="info-sign")
    ).add_to(m)

    # ------------------------
    # Layer control and save
    # ------------------------
    folium.LayerControl(collapsed=False).add_to(m)

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    outpath = output_dir / "diridon_interactive_map.html"
    m.save(outpath)

    print(f"✓ Interactive map saved to: {outpath}")
    return outpath


# ---------------------------------------------
# Step 8: Export Outputs
# ---------------------------------------------

def export_outputs(within_1m, uv, tracts_sel, acs_summary, output_dir):
    """
    Export analysis results to files.
    
    Note: Data will be saved in EPSG:3857. Convert back to EPSG:4326 
    before final export if needed for compatibility.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Convert back to EPSG:4326 for storage/sharing
    within_1m_geo = within_1m.to_crs("EPSG:4326")
    uv_geo = uv.to_crs("EPSG:4326")
    tracts_sel_geo = tracts_sel.to_crs("EPSG:4326")

    p1 = output_dir / "diridon_parcels_1mile.parquet"
    within_1m_geo.to_parquet(p1)

    p2 = output_dir / "diridon_uv_parcels_1mile.parquet"
    uv_geo.to_parquet(p2)

    p3 = output_dir / "diridon_tracts_2mile.parquet"
    tracts_sel_geo.to_parquet(p3)

    acs_df = pd.DataFrame([acs_summary])
    acs_out = output_dir / "diridon_acs_2mile_summary.csv"
    acs_df.to_csv(acs_out, index=False)

    print(f"✓ Exported {len(within_1m)} parcels within 1 mile")
    print(f"✓ Exported {len(uv)} Urban Village parcels within 1 mile")
    print(f"✓ Exported {len(tracts_sel)} census tracts within 2 miles")
    print(f"✓ Exported ACS summary statistics")

    return {
        "parcels_1m": p1,
        "uv_1m": p2,
        "tracts_2m": p3,
        "acs_summary": acs_out
    }