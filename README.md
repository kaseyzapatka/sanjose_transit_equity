# San Jose Transit Equity  

This project looks at transportation equity in downtown San Jose. 

## Prepare virtual environment
I created a virtual environment specifically for this exercise. You can recreate it running `pip install -r requirements.txt`. The python version used is `Python 3.13.5`. You need to first create a clean working environment. With `conda` you can do this by running this command in a terminal `conda create -n sanjose_case_study python=3.13.5` and then running `conda activate sanjose_case_study`. 

## Original data sources

- All shapefiles for San Jose City can be found at this [link](https://data.sanjoseca.gov/organization/maps-data). 
- [Parcel shapefiles](https://data.sanjoseca.gov/dataset/parcels/resource/3e23d2d0-e07d-4d13-addd-608cc3221bd5). 
- [Zoning shapefiles](https://data.sanjoseca.gov/dataset/zoning-districts/resource/3e2aacc3-f608-483e-85c6-f1be7e1e4995). 
- San Jose City [zoning codes](https://library.municode.com/ca/san_jose/codes/code_of_ordinances?nodeId=TIT20ZO_CH20.10GEPRZODI_20.10.060ZODIES)

## Data
Since GitHub has data limitations, all data used for this project can be accessed [here](https://drive.google.com/drive/folders/1rM17LTuIoiBh7mqlefV8dIxEGZeKY9fc?usp=sharing). 

## Running this analysis
All coded needed to reproduce this analysis is in `code.ipynb` file. The `utils.py` file provides functions called in the analysis. 
