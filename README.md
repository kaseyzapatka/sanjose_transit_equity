# Housing Capacity & Equity Around Diridon Station

A parcel-level analysis of how much housing San José's zoning permits within one
mile of **Diridon Station** — the Bay Area's flagship transit hub — and who
lives there now. The analysis is packaged as an illustrated **Quarto website +
PDF memo**.

**Read it:** <https://www.kaseyzapatka.com/sanjose_transit_equity>

## What it finds

- **≈ 88,000** homes of theoretical zoned capacity within 1 mile, **~90% in the
  Downtown core** (governed by the Diridon Station Area Plan, not the citywide
  mixed-use zones).
- **≈ 34,600** net-new homes if only currently **underbuilt "soft sites"**
  (vacant lots / surface parking) redevelop — **~42% of Downtown's zoned land**
  is still vacant or surface parking.
- The station area is **64% renters** and **~2× as transit-dependent** as the
  city, so the recommendation pairs upzoning with anti-displacement tools.

## Pipeline

Processed data must be present in `data/processed/` (see **Data** below). Then
run, in order, from `code/`:

```bash
python 02_diridon_capacity.py     # 1-mile capacity + soft sites (Title 20 / DSAP densities, OSM footprints)
python 03_diridon_equity.py       # ACS + Equity Index overlay; displacement-vulnerability flags
python 04_diridon_figures.py      # static figures (hero map, capacity bar, who-lives-here)
python 05_diridon_interactive.py  # interactive Leaflet/folium hero map
```

Each step writes to `output/tables/` and `output/maps/`, which the memo reads.
`code/01_data_pipeline.py` (+ `functions.py`) is the upstream ETL that builds
`data/processed/` from the raw San José open-data shapefiles and ACS.

## Build the memo + site

The site is a Quarto website rendered to `docs/` (served by GitHub Pages). A
post-render hook produces the PDF via Quarto's bundled **Typst** (no LaTeX
needed):

```bash
quarto render        # builds docs/ (HTML site) + docs/diridon_capacity_equity_memo.pdf
quarto preview       # live local preview
```

Pages: repository **Settings → Pages → Deploy from branch → `main` / `docs`**.

## Data sources

- City of San José Open Data — parcels, zoning districts, Equity Index census
  tracts, affordable rental housing (<https://data.sanjoseca.gov/organization/maps-data>)
- San José Municipal Code **Title 20** (Zoning), Ch. 20.55, Table 20-136
- **Diridon Station Area Plan** (amended 2021) and General Plan land use designations
- **ACS 2022** 5-year estimates via `pygris` / Census API
- **OpenStreetMap** building footprints (© OpenStreetMap contributors, ODbL)

Large data files are kept off GitHub; download them here:
<https://drive.google.com/drive/folders/1rM17LTuIoiBh7mqlefV8dIxEGZeKY9fc?usp=sharing>

## Environment

Python 3.13 with `geopandas`, `pygris`, `folium`, `matplotlib`, `seaborn`,
`pyarrow`, `requests`. See `requirements.txt`. Rendering the site additionally
requires [Quarto](https://quarto.org) ≥ 1.4.

## Repository layout

```
code/                     analysis modules (capacity, equity, figures, interactive) + ETL
data/processed/           processed parcels, zoning, tracts, equity (off-GitHub)
output/figures/           static figures (PNG + PDF)
output/tables/            capacity & equity summary tables (CSV)
output/maps/              parcel/tract layers + interactive map
index.qmd                 the memo (main page)
map.qmd, methods.qmd      interactive map + methods/sources pages
_quarto.yml               website config (output-dir: docs)
scripts/render_pdfs.sh    post-render hook -> PDF via Typst
docs/                     rendered site (GitHub Pages)
```

## Caveats

Capacity is **theoretical zoned capacity**, not a production forecast. Soft sites
are an open-data **footprint-coverage proxy** (not assessor improvement value),
so the Downtown figure is a ceiling. Vulnerability flags measure *exposure*, not
predicted displacement. See the memo's *Methods & Sources* page for full detail.
