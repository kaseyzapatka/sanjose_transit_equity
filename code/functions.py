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

# for data management
import os
import geopandas as gpd
import pandas as pd
# for data visualization 
import seaborn as sns
from shapely.geometry import Point
import matplotlib.pyplot as plt
import mapclassify
# for census analysis 
from pygris import tracts, places, validate_state
from pygris.data import get_census


# ==========================================================================
# EXTRACT FUNCTIONS
# ==========================================================================

def load_parcels(path="../data/raw/Parcels/Parcels.shp"):
    return gpd.read_file(path)

def load_zoning(path="../data/raw/Zoning_Districts/Zoning_Districts.shp"):
    return gpd.read_file(path)

#def load_railroad(path="../data/Railroad/Railroad.shp"):
#    rr = gpd.read_file(path)
#    return rr.query("NAME != 'Union Pacific'")

#def load_bikeways(path="../data/Bikeways/Bikeways.shp"):
#    return gpd.read_file(path)

#def load_bike_racks(path="../data/Bike_Racks/Bike_Racks.shp"):
#    return gpd.read_file(path)

def load_affordable_housing(path="../data/raw/Affordable_Rental_Housing/Affordable_Rental_Housing.shp"):
    return gpd.read_file(path)

def load_equity_index(path="../data/raw/Equity_Index_Census_Tracts/Equity_Index_Census_Tracts.shp"):
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

def attach_tract_data_to_parcels(parcels_gdf, tracts_gdf, tract_fields=None):
    """
    Spatially join parcels to census tracts, attaching selected tract-level fields
    to each parcel. Ensures GEOID is always included and correctly named.
    """
    import geopandas as gpd

    if tract_fields is None:
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

    # Ensure CRS match
    if parcels_gdf.crs != tracts_gdf.crs:
        tracts_gdf = tracts_gdf.to_crs(parcels_gdf.crs)

    # Keep only requested columns + GEOID
    fields_to_keep = ["GEOID"] + [f for f in tract_fields if f in tracts_gdf.columns]
    tracts_subset = tracts_gdf[["geometry"] + fields_to_keep].copy()

    # Spatial join
    parcels_with_tract_data = gpd.sjoin(
        parcels_gdf.set_geometry("geometry"),
        tracts_subset.set_geometry("geometry"),
        how="left",
        predicate="within"
    )

    # Drop extra geometry column and rename GEOID
    parcels_with_tract_data = parcels_with_tract_data.drop(columns=["index_right"], errors="ignore")
    
    # If GEOID got a suffix, rename to plain 'GEOID'
    for col in parcels_with_tract_data.columns:
        if col.endswith("_right") and col.startswith("GEOID"):
            parcels_with_tract_data = parcels_with_tract_data.rename(columns={col: "GEOID"})

    return parcels_with_tract_data


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
    filename="../output/maps/choropleth_map.pdf", # file name
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


import geopandas as gpd
from pygris import tracts, places, validate_state
from pygris.data import get_census


# ======================================================
#  CENSUS EXTRACTION
# ======================================================
#
# 1. Get Census data
# ----------------------------------------
def pull_acs_data(state="CA", year=2022):
    """Pull ACS 5-year data for a given state."""
    acs_vars = {
        "median_age": "B01002_001E",
        "median_income": "B19013_001E",

        # Rent burden
        "total_renter_households": "B25070_001E",
        "rent_<15": "B25070_002E",
        "rent_15_19": "B25070_003E",
        "rent_20_24": "B25070_004E",
        "rent_25_29": "B25070_005E",
        "rent_30_34": "B25070_006E",
        "rent_35_39": "B25070_007E",
        "rent_40_49": "B25070_008E",
        "rent_50_plus": "B25070_009E",

        # Poverty
        "poverty_universe": "B17001_001E",
        "below_poverty": "B17001_002E",

        # Vehicle availability
        "total_households": "B08201_001E",
        "no_vehicle": "B08201_002E",

        # Tenure (renters vs owners)
        "tenure_total": "B25003_001E",
        "owner_occupied": "B25003_002E",
        "renter_occupied": "B25003_003E",

        # commute mode (B08301)
        "total_workers": "B08301_001E",
        "drove": "B08301_002E",
        "public_transit_total": "B08301_010E",
        "bus": "B08301_011E",
        "subway": "B08301_012E",
        "commuter_rail": "B08301_013E",  # long-distance train or commuter rail
        "light_rail": "B08301_014E",  # long-distance train or commuter rail
        "bike": "B08301_018E",
        "walked": "B08301_019E",
        "worked_home": "B08301_021E",

        # Median gross rent
        "median_rent": "B25064_001E",

        # Units in structure (B25024)
        "units_total": "B25024_001E",
        "units_1_detached": "B25024_002E",
        "units_1_attached": "B25024_003E",
        "units_2": "B25024_004E",
        "units_3_4": "B25024_005E",
        "units_5_9": "B25024_006E",
        "units_10_19": "B25024_007E",
        "units_20_49": "B25024_008E",
        "units_50_plus": "B25024_009E",
        "units_mobile": "B25024_010E",
        "units_other": "B25024_011E",

        # vacancy rate (B25002)
        "housing_units_total": "B25002_001E",
        "housing_units_occupied": "B25002_002E",
        "housing_units_vacant": "B25002_003E",

        # race/ethnicity (B02001 & B03003)
        "race_total": "B02001_001E",
        "white": "B02001_002E",
        "black": "B02001_003E",
        "asian": "B02001_005E",
        "hisp_total": "B03003_001E",
        "hispanic": "B03003_003E",

        # education (B15003)
        "edu_total": "B15003_001E",
        "bachelors": "B15003_022E",
        "masters": "B15003_023E",
        "professional": "B15003_024E",
        "doctorate": "B15003_025E",

        # Income inequality
        "gini": "B19083_001E"
    }

    df = get_census(
        dataset="acs/acs5",
        variables=list(acs_vars.values()),
        year=year,
        params={"for": "tract:*", "in": f"state:{validate_state(state)}"},
        guess_dtypes=True,
        return_geoid=True
    )

    # rename
    df = df.rename(columns={v: k for k, v in acs_vars.items()})
    return df


#
# 2. Create indicators
# ----------------------------------------
def compute_acs_indicators(df):
    """Compute all ACS derived variables (rent burden, poverty, tenure, etc.)."""

    # RENT BURDEN
    df["rent_burdened_count"] = (
        df["rent_30_34"] + df["rent_35_39"] +
        df["rent_40_49"] + df["rent_50_plus"]
    )
    df["rent_burdened_pct"] = df["rent_burdened_count"] / df["total_renter_households"] * 100

    # POVERTY
    df["poverty_rate"] = df["below_poverty"] / df["poverty_universe"] * 100

    # TENURE
    df["pct_renters"] = df["renter_occupied"] / df["tenure_total"] * 100
    df["pct_homeowners"] = df["owner_occupied"] / df["tenure_total"] * 100

    # TRANSPORTATION
    df["no_vehicle_pct"] = df["no_vehicle"] / df["total_households"] * 100
    df["public_transit_pct"] = df["public_transit_total"] / df["total_workers"] * 100
    df["drove_pct"] = df["drove"] / df["total_workers"] * 100
    df["bike_pct"] = df["bike"] / df["total_workers"] * 100
    df["walked_pct"] = df["walked"] / df["total_workers"] * 100
    df["commuter_rail_pct"] = df["commuter_rail"] / df["total_workers"] * 100
    df["light_rail_pct"] = df["light_rail"] / df["total_workers"] * 100
    df["worked_home_pct"] = df["worked_home"] / df["total_workers"] * 100

    # HOUSING STRUCTURE
    df["single_family_units"] = df["units_1_detached"] + df["units_1_attached"]
    df["small_multifamily_units"] = df["units_2"] + df["units_3_4"]
    df["medium_multifamily_units"] = df["units_5_9"] + df["units_10_19"]
    df["large_multifamily_units"] = df["units_20_49"] + df["units_50_plus"]
    df["other_units"] = df["units_mobile"] + df["units_other"]

    df["pct_single_family"] = df["single_family_units"] / df["units_total"] * 100
    df["pct_small_multifamily"] = df["small_multifamily_units"] / df["units_total"] * 100
    df["pct_medium_multifamily"] = df["medium_multifamily_units"] / df["units_total"] * 100
    df["pct_large_multifamily"] = df["large_multifamily_units"] / df["units_total"] * 100
    df["pct_other"] = df["other_units"] / df["units_total"] * 100

    # VACANCY
    df["vacancy_rate"] = df["housing_units_vacant"] / df["housing_units_total"] * 100

    # RACE / ETHNICITY
    df["pct_white"] = df["white"] / df["race_total"] * 100
    df["pct_black"] = df["black"] / df["race_total"] * 100
    df["pct_asian"] = df["asian"] / df["race_total"] * 100
    df["pct_latino"] = df["hispanic"] / df["hisp_total"] * 100

    # EDUCATION
    df["college_plus"] = df["bachelors"] + df["masters"] + df["professional"] + df["doctorate"]
    df["pct_college_plus"] = df["college_plus"] / df["edu_total"] * 100

    return df

#
# 3. Pull tracts and places
# ----------------------------------------
def pull_tracts(state="CA", year=2022):
    return tracts(state=state, cb=True, year=year, cache=True)


def pull_places(state="CA", year=2022):
    return places(state=state, cb=True, year=year, cache=True)

#
# 4. Subset tracts to a city
# ----------------------------------------
def subset_city_tracts(tracts_gdf, places_gdf, city_name):
    """Subset tracts whose *centroids* fall inside the named city."""
    city = places_gdf[places_gdf["NAME"] == city_name]

    tracts_copy = tracts_gdf.copy()
    tracts_copy["centroid"] = tracts_copy.centroid

    subset = gpd.sjoin(
        tracts_copy.set_geometry("centroid"),
        city,
        predicate="within"
    ).drop(columns="geometry")

    # restore original geometry
    subset = subset.set_geometry(tracts_gdf.geometry)
    return subset


#
# 5. Merge tracts and Census data
# ----------------------------------------

def merge_tracts_with_acs(tracts_city, acs_df):
    return tracts_city.merge(
        acs_df,
        left_on="GEOID_left",
        right_on="GEOID",
        how="left"
    )

# ======================================================
#  ZONING ABBREVIATIONS
# ======================================================

#
# Create zoning abbreviation dictionary
# ----------------------------------------
zoning_abb = {
    "UV": "Urban Village",
    "UVC": "Urban Village Commercial",
    "UR": "Urban Residential",
    "TR": "Transit Residential",
    "MU": "Mixed Use",
    "MUC": "Mixed Use Commercial",
    "MUN": "Municipal/Neighborhood Mixed Use",
}

#
# Apply classification dictionary
# ----------------------------------------
def abbreviate_zoning(code):
    if pd.isna(code):
        return "Unknown"
    return zoning_abb.get(code.strip(), "Other")

# ======================================================
#  ZONING CLASSIFICATIONS
# ======================================================

#
# Create classification dictionary
# ----------------------------------------
zoning_classification = {
    # -------------------------
    # RESIDENTIAL
    # -------------------------
    "R-1-1": "Residential",
    "R-1-2": "Residential",
    "R-1-5": "Residential",
    "R-1-10": "Residential",
    "R-1-8": "Residential",
    "R-1-RR": "Residential",      # Rural Residential
    "R-2": "Residential",
    "R-M": "Residential",
    "R-MH": "Residential",
    "MS-C": "Residential",        # Mobilehome Subdistrict
    "MS-G": "Residential",

    # RESIDENTIAL PD / CL overlays
    "R-2(PD)": "Residential",
    "R-1-1(PD)": "Residential",
    "R-1-2(PD)": "Residential",
    "R-1-5(PD)": "Residential",
    "R-1-5(CL)": "Residential",
    "R-1-8(PD)": "Residential",
    "R-1-8(CL)": "Residential",
    "R-M(PD)": "Residential",
    "R-M(CL)": "Residential",
    
    # -------------------------
    # COMMERCIAL
    # -------------------------
    "C-1": "Commercial",
    "C-2": "Commercial",
    "CP": "Commercial",
    "CN": "Commercial",
    "CG": "Commercial",
    "CR": "Commercial",
    "CO": "Commercial",
    "CIC": "Commercial",          # Commercial Industrial Combined
    "TEC": "Commercial",          # Technology Park
    "DC": "Commercial",           # Downtown Core commercial
    "DC-NT1": "Commercial",       # Downtown Neighborhood Transition

    # COMMERCIAL PD overlays
    "CG(PD)": "Commercial",
    "CN(PD)": "Commercial",
    "CP(PD)": "Commercial",
    "CIC(PD)": "Commercial",
    "CO(PD)": "Commercial",
    "DC(PD)": "Commercial",
    "TEC(PD)": "Commercial",
    "LI(PD)": "Industrial",
    "HI(PD)": "Industrial",
    "IP(PD)": "Industrial",
    
    # -------------------------
    # INDUSTRIAL
    # -------------------------
    "LI": "Industrial",
    "HI": "Industrial",
    "IP": "Industrial",

    # -------------------------
    # MIXED USE / URBAN VILLAGE / TRANSIT
    # -------------------------
    "MUN": "Mixed Use",
    "MUC": "Mixed Use",
    "UV": "Mixed Use",
    "UVC": "Mixed Use",
    "UR": "Mixed Use",            # Urban Residential
    "TR": "Mixed Use",            # Transit Residential

    # Mixed Use PD overlays
    "MUN(PD)": "Mixed Use",
    "UR(PD)": "Mixed Use",

    # -------------------------
    # SPECIAL PURPOSE
    # -------------------------
    "OS": "Special Purpose",
    "A": "Special Purpose",
    "PQ": "Special Purpose",
    "PQP": "Special Purpose",     # Public / Quasi-Public
    "PF": "Special Purpose",
    "PI": "Special Purpose",
    "WATER": "Special Purpose",

    # SPECIAL PURPOSE PD overlays
    "OS(PD)": "Special Purpose",
    "A(PD)": "Special Purpose",
    "PQP(PD)": "Special Purpose",

    # -------------------------
    # CLEAN FILL FOR ANYTHING ELSE
    # -------------------------
    "OTHER": "Other"
}

#
# Apply classification dictionary
# ----------------------------------------
def classify_zoning(code):
    if pd.isna(code):
        return "Unknown"
    return zoning_classification.get(code.strip(), "Other")
