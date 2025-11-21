# ==========================================================================
# functions.py
#
# Summary:
# Helper functions for the two-file ETL pipeline.
# Provides:
#   • extract/load functions for reading spatial datasets
#   • transform functions for joining and deduplication
#   • load/output functions for saving GeoJSON and Parquet
#   • an optional choropleth map helper
# ==========================================================================

# import necessary libraries
import os
import geopandas as gpd
import pandas as pd
import seaborn as sns
from shapely.geometry import Point
import matplotlib.pyplot as plt
import mapclassify


# ==========================================================================
# EXTRACT FUNCTIONS
# ==========================================================================

def load_parcels(path="../data/Parcels/Parcels.shp"):
    return gpd.read_file(path)

def load_zoning(path="../data/Zoning_Districts/Zoning_Districts.shp"):
    return gpd.read_file(path)

def load_railroad(path="../data/Railroad/Railroad.shp"):
    rr = gpd.read_file(path)
    return rr.query("NAME != 'Union Pacific'")

def load_bikeways(path="../data/Bikeways/Bikeways.shp"):
    return gpd.read_file(path)

def load_bike_racks(path="../data/Bike_Racks/Bike_Racks.shp"):
    return gpd.read_file(path)

def load_affordable_housing(path="../data/Affordable_Rental_Housing/Affordable_Rental_Housing.shp"):
    return gpd.read_file(path)

def load_equity_index(path="../data/Equity_Index_Census_Tracts/Equity_Index_Census_Tracts.shp"):
    return gpd.read_file(path)


# ==========================================================================
# TRANSFORM FUNCTIONS
# ==========================================================================

def join_parcels_zoning(parcels, zoning):
    """
    Perform spatial join assigning each parcel to the zoning district with the
    largest geometric overlap, deduplicate, then select parcel with largest overlap
    so that each parcel has one zoning assignment.

    Returns:
        GeoDataFrame with unique parcels and zoning attributes.
    """
    
    # Spatial join — returns multiple rows per parcel if multiple zoning overlaps
    joined = sjoin_parcels_to_zd(parcels, zoning, how="largest")

    # Deduplicate — keep only the zoning record with the largest overlap_area
    cleaned = (
        joined
        .sort_values(["PARCELID", "overlap_area"], ascending=[True, False])
        .drop_duplicates(subset="PARCELID", keep="first")
        .reset_index(drop=True)
    )

    return cleaned



# ==========================================================================
# LOAD FUNCTIONS
# ==========================================================================

# save as parquet which is slightly more efficient 
def save_parquet(gdf, path="../output/output.parquet"):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    gdf.to_parquet(path, index=False, engine="pyarrow")


# ==========================================================================
# HELPER FUNCTIONS
# ==========================================================================

#
# Spatial join function
# --------------------------------------
# Join parcels to zoning data by selecting the zoning category with the 
# largest overlap of area on a parcel.

# import necessary libraries
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import geopandas as gpd
from shapely.geometry import Point


# define spatial join function
def sjoin_parcels_to_zd(parcels, zoning, how="largest"):
    
    # ensure same CRS
    if parcels.crs != zoning.crs:
        zoning = zoning.to_crs(parcels.crs)
    
    # spatial join (parcels will appear multiple times if overlapping multiple zoning districts)
    joined = gpd.sjoin(parcels, zoning, how="left", predicate="intersects")
    
    if how == "largest":
        # compute intersection area safely
        def get_overlap_area(row):
            if pd.isna(row.index_right):  # no zoning match
                return 0
            return row.geometry.intersection(zoning.loc[row.index_right, "geometry"]).area
        
        joined["overlap_area"] = joined.apply(get_overlap_area, axis=1)
    
    elif how == "first":
        # keep only the first zoning district encountered per parcel
        joined = joined.loc[~joined.index.duplicated(keep="first")]
    
    else:
        raise ValueError("how must be 'largest' or 'first'")
    
    return joined.drop(columns=["index_right"], errors="ignore")



#
# Flexible mapping function
# ----------------------------------------
# Create a choropleth map that plots a station point and buffer overlays.
# Can optionally save the figure to a file and add notes at the bottom.

# libraries
import matplotlib.pyplot as plt
import mapclassify

# mapping function
def choropleth_map(
    gdf,                                    # data with geometry
    column,                                 # column to display
    title,                                  # title
    station_gdf=None,                       # station overlay
    buffer_gdf=None,                        # buffer overlay
    k=5,                                    # number of classes
    cmap="Blues",                           # color scheme
    save=False,                             # save figure
    filename="../output/choropleth_map.pdf", # file name
    notes=None                              # notes
):    
    # ensure consistent CRS
    gdf = gdf.to_crs(epsg=4269)
    if station_gdf is not None:
        station_gdf = station_gdf.to_crs(epsg=4269)
    if buffer_gdf is not None:
        buffer_gdf = buffer_gdf.to_crs(epsg=4269)
    
    fig, ax = plt.subplots(figsize=(10, 10))
    ax.axis("off")
    
    # main choropleth map
    gdf.plot(
        ax=ax,
        column=column,
        scheme="NaturalBreaks",
        k=k,
        legend=True,
        cmap=cmap,
        edgecolor="black",
        linewidth=0.5
    )
    
    # station overlay
    if station_gdf is not None:
        station_gdf.plot(
            ax=ax, color="black", marker=".", markersize=100, label="Station"
        )
    
    # 2 mile radius overlay
    if buffer_gdf is not None:
        buffer_gdf.boundary.plot(
            ax=ax, color="blue", linestyle="--", linewidth=2, label="Buffer"
        )
    
    # title
    ax.set_title(title, fontsize=14)
    
    # footnotes 
    if notes:
        fig.text(0.1, 0.01, notes, ha='left', fontsize=10)
    
    # save
    if save:
        plt.savefig(filename, format="pdf", bbox_inches="tight")
        print(f"Figure saved as {filename}")

    
    plt.show()
