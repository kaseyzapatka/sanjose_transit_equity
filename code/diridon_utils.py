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

# ---------------------------------------------
# Configuration
# ---------------------------------------------

DIRIDON_LON_LAT = (-121.9036, 37.3292)
MILE_IN_METERS = 1609.344


# ---------------------------------------------
# Step 1: Load Data
# ---------------------------------------------

def load_data(parcels_path="../data/processed/parcels_with_zoning.parquet", 
              tracts_path="../data/processed/san_jose_tracts_with_acs.geoparquet"):
    parcels = gpd.read_parquet(parcels_path)
    tracts = gpd.read_parquet(tracts_path)
    return parcels, tracts


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
# Step 6: Maps
# ---------------------------------------------

def create_maps(parcels, tracts, output_dir):
    """
    Create map of parcels and tracts near Diridon Station with 1-mile and 2-mile buffers.
    
    Parameters:
    -----------
    parcels : GeoDataFrame (any CRS - will be reprojected)
    tracts : GeoDataFrame (any CRS - will be reprojected)
    output_dir : str or Path
    
    Returns:
    --------
    Path : path to saved map file
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. Reproject everything to EPSG:3857 for consistent analysis
    parcels_proj = parcels.to_crs(epsg=3857)
    tracts_proj = tracts.to_crs(epsg=3857)
    
    # 2. Use the constant for Diridon Station coordinates
    diridon_station = gpd.GeoDataFrame(
        geometry=[Point(*DIRIDON_LON_LAT)],  # ✅ Use constant
        crs="EPSG:4326"
    ).to_crs(epsg=3857)
    
    # 3. Create buffers (in meters)
    buffer_1mile = diridon_station.buffer(MILE_IN_METERS)
    buffer_2mile = diridon_station.buffer(2 * MILE_IN_METERS)
    
    # 4. Subset parcels within buffers
    parcels_within_2mile = gpd.overlay(
        parcels_proj, 
        gpd.GeoDataFrame(geometry=buffer_2mile, crs=parcels_proj.crs), 
        how="intersection"
    )
    
    parcels_within_1mile = gpd.overlay(
        parcels_proj, 
        gpd.GeoDataFrame(geometry=buffer_1mile, crs=parcels_proj.crs), 
        how="intersection"
    )
    
    # 5. Identify urban-zoned parcels within 1 mile and assign colors
    urban_zoning = ["Urban Village", "Urban Village Commercial", "Urban Residential", "Transit Residential", "Mixed Use", "Mixed Use Commercial", "Municipal/Neighborhood Mixed Use"]
    uz_within_1mile = parcels_within_1mile[
        parcels_within_1mile["zoning"].isin(urban_zoning)
    ].copy()
    
    # Define color scheme for each zoning type
    zoning_colors = {
        "Urban Village": "#e41a1c",                    # Red
        "Urban Village Commercial": "#377eb8",         # Blue
        "Urban Residential": "#4daf4a",                # Green
        "Transit Residential": "#984ea3",              # Purple
        "Mixed Use": "#ff7f00",                        # Orange
        "Mixed Use Commercial": "#ffff33",             # Yellow
        "Municipal/Neighborhood Mixed Use": "#a65628"  # Brown
    }
    
    # 6. Create the map
    fig, ax = plt.subplots(figsize=(14, 12))
    
    # Add census tracts as background context
    tracts_proj.plot(ax=ax, color="lightgrey", edgecolor="grey", alpha=0.3)
    
    # Add 2-mile buffer boundary (context)
    gpd.GeoDataFrame(geometry=buffer_2mile, crs=parcels_proj.crs).boundary.plot(
        ax=ax, color="grey", linestyle="--", linewidth=0.5, label="2-mile radius"
    )
    
    # Add parcel base map (within 2-mile area)
    parcels_within_2mile.plot(ax=ax, color="lightgrey", edgecolor="white", linewidth=0.1)
    
    # Plot each zoning type with its own color
    for zone_type in urban_zoning:
        zone_parcels = uz_within_1mile[uz_within_1mile["zoning"] == zone_type]
        if len(zone_parcels) > 0:
            zone_parcels.plot(
                ax=ax, 
                color=zoning_colors[zone_type], 
                alpha=0.7, 
                edgecolor="black", 
                linewidth=0.2,
                label=f"{zone_type} ({len(zone_parcels)} parcels)"
            )
    
    # Add 1-mile buffer boundary
    gpd.GeoDataFrame(geometry=buffer_1mile, crs=parcels_proj.crs).boundary.plot(
        ax=ax, color="black", linestyle="--", linewidth=1, label="1-mile radius"
    )
    
    # Add station marker
    diridon_station.plot(
        ax=ax, color="black", marker=".", markersize=200, 
        label="Diridon Station", zorder=10
    )
    
    # Set the map extent to 2-mile buffer bounds (with small padding)
    buffer_2m_bounds = gpd.GeoDataFrame(geometry=buffer_2mile, crs=parcels_proj.crs).total_bounds
    padding = MILE_IN_METERS * 0.1  # 10% padding
    ax.set_xlim(buffer_2m_bounds[0] - padding, buffer_2m_bounds[2] + padding)
    ax.set_ylim(buffer_2m_bounds[1] - padding, buffer_2m_bounds[3] + padding)
    
    # Format
    ax.set_title(
        "Urban-zoned parcels within 1 mile of San Jose Diridon Station", 
        fontsize=14, fontweight="bold", pad=20
    )
    
    # Create custom legend handles
    legend_handles = [
        Patch(facecolor=zoning_colors[z], edgecolor="black", label=f"{z}")
        for z in urban_zoning
        if z in uz_within_1mile["zoning"].unique()
    ]

    # Add buffer + station handles manually
    legend_handles.extend([
        Patch(facecolor="none", edgecolor="black", linestyle="--", label="1-mile radius"),
        Patch(facecolor="none", edgecolor="grey", linestyle="--", label="2-mile radius"),
    ])

    # Add station marker
    station_handle = plt.Line2D(
        [0], [0],
        marker=".",
        color="black",
        markersize=12,
        linestyle="None",
        label="Diridon Station"
    )
    legend_handles.append(station_handle)

    # Now use manual legend
    ax.legend(
        handles=legend_handles,
        loc="upper right",
        title="Legend",
        title_fontsize=10,
        fontsize=9,
        framealpha=0.95
    )
    # Create footnote with parcel counts
    total_parcels_within_1mile = len(parcels_within_1mile)

    footnote_lines = ["Share of urban-zoned parcels within 1 mile of San Jose Dirdon Station (count):"]
    for zone_type in urban_zoning:
        count = len(uz_within_1mile[uz_within_1mile["zoning"] == zone_type])
        if count > 0:
            share = count / total_parcels_within_1mile * 100
            footnote_lines.append(f"  {zone_type}: {share:.1f}% ({count:,} parcels)")

    # Add total parcels within 1 mile
    footnote_lines.append(f"Total parcels: {total_parcels_within_1mile:,}")

    footnote_text = "\n".join(footnote_lines)
    fig.text(
        0.12, 0.02, footnote_text,
        ha='left', va='bottom', fontsize=8,
    )
    ax.axis('off')
    
    # 7. Save the map
    outpath = output_dir / "diridon_buffer_map.png"
    plt.savefig(outpath, dpi=150, bbox_inches='tight')
    plt.close()
    
    print(f"✓ Map saved to: {outpath}")
    print(f"✓ Zoning breakdown:")
    for zone_type in urban_zoning:
        count = len(uz_within_1mile[uz_within_1mile["zoning"] == zone_type])
        if count > 0:
            print(f"  - {zone_type}: {count} parcels")
    
    return outpath


# ---------------------------------------------
# Step 7: Export Outputs
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