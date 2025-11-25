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

# ---------------------------------------------
# Configuration
# ---------------------------------------------

DIRIDON_LON_LAT = (-121.9036, 37.3292)
MILE_IN_METERS = 1609.344


# ---------------------------------------------
# Step 1: Load Data
# ---------------------------------------------

def load_data(parcels_path="../output/parcels_with_zoning.parquet", 
              tracts_path="../output/san_jose_tracts_with_acs.geoparquet"):
    parcels = gpd.read_parquet(parcels_path)
    tracts = gpd.read_parquet(tracts_path)
    return parcels, tracts
    


# ---------------------------------------------
# Step 2: Reproject
# ---------------------------------------------

def reproject_for_buffering(parcels, tracts):
    target_crs = "EPSG:3857"
    return parcels.to_crs(target_crs), tracts.to_crs(target_crs)


# ---------------------------------------------
# Step 3: Build Diridon Buffers
# ---------------------------------------------

def build_diridon_buffers():
    pt = gpd.GeoSeries([Point(*DIRIDON_LON_LAT)], crs="EPSG:4326")
    pt_m = pt.to_crs("EPSG:3857").iloc[0]
    buffer_1m = pt_m.buffer(1 * MILE_IN_METERS)
    buffer_2m = pt_m.buffer(2 * MILE_IN_METERS)
    return pt_m, buffer_1m, buffer_2m


# ---------------------------------------------
# Step 4: Parcel Summary
# ---------------------------------------------

def summarize_parcels(parcels_m, buffer_1m):
    parcels_m["centroid"] = parcels_m.geometry.centroid
    centroids = parcels_m.set_geometry("centroid")

    within_1m = centroids[centroids.centroid.within(buffer_1m)]
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
    parcels : GeoDataFrame
        Parcels with zoning information
    tracts : GeoDataFrame
        Census tracts with ACS data
    output_dir : str or Path
        Directory to save output map file
    
    Returns:
    --------
    Path : path to saved map file
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. Ensure everything in projected CRS (meters)
    parcels_proj = parcels.to_crs(epsg=3857)
    
    # 2. Diridon Station coordinates (lon, lat in WGS84)
    diridon_station = gpd.GeoDataFrame(
        geometry=[Point(-121.9028, 37.3292)],
        crs="EPSG:4326"
    ).to_crs(epsg=3857)
    
    # 3. Create buffers
    buffer_1mile = diridon_station.buffer(1609.34)   # 1 mile in meters
    buffer_2mile = diridon_station.buffer(2 * 1609.34)  # 2 miles in meters
    
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
    
    # 5. Identify urban-zoned parcels within 1 mile
    urban_zoning = ["UV", "UVC", "UR", "TR", "MU", "MUC", "MUN"]
    uv_within_1mile = parcels_within_1mile[parcels_within_1mile["ZONING"].isin(urban_zoning)]

    
    # 6. Create the map
    fig, ax = plt.subplots(figsize=(10, 10))
    
    # Add parcel base map (zoomed into 2-mile area)
    parcels_within_2mile.plot(ax=ax, color="lightgrey", edgecolor="white")
    
    # Highlight urban-zoned parcels within 1 mile
    uv_within_1mile.plot(ax=ax, color="red", alpha=0.7)
    
    # Add 1-mile buffer boundary
    gpd.GeoDataFrame(geometry=buffer_1mile, crs=parcels_proj.crs).boundary.plot(
        ax=ax, color="blue", linestyle="--", linewidth=2, label="1-mile radius"
    )
    
    # Add station marker
    diridon_station.plot(ax=ax, color="black", marker=".", markersize=100, label="Diridon Station")
    
    # Format
    ax.set_title("Urban-zoned, mixed use parcels within 1 mile of San Jose Diridon Station", fontsize=14)
    ax.legend()
    ax.axis('off')
    
    # 7. Save the map
    outpath = output_dir / "diridon_buffer_map.png"
    plt.savefig(outpath, dpi=150, bbox_inches='tight')
    plt.close()
    
    return outpath


# ---------------------------------------------
# Step 7: Export Outputs
# ---------------------------------------------

def export_outputs(within_1m, uv, tracts_sel, acs_summary, output_dir):
    output_dir = Path(output_dir)

    p1 = output_dir / "diridon_parcels_1mile.parquet"
    within_1m.to_parquet(p1)

    p2 = output_dir / "diridon_uv_parcels_1mile.parquet"
    uv.to_parquet(p2)

    acs_df = pd.DataFrame([acs_summary])
    acs_out = output_dir / "diridon_acs_2mile_summary.csv"
    acs_df.to_csv(acs_out, index=False)

    return {
        "parcels_1m": p1,
        "uv_1m": p2,
        "acs_summary": acs_out
    }
