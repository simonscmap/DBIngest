# Dataset File Strucure
This document describes the common file structure to store the data/meta-data produced by the labs funded by the Simons Foundation, Oceanography program. The file format is excel spreadsheet. Data is stored in the first sheet and the sheet title is "data". The second sheet stores the dataset meta-data and is called "dataset_meta_data". Meta-data associated with the variables in the dataset are kept in the third sheet, "vars_meta_data". An example template can be found [here](https://github.com/simonscmap/DBIngest/raw/master/template/datasetTemplate.xlsx).


## Dataset Filename Convention
Dataset filename:
`<dataset_short_name>`\_`<dataset_release_data>`\_v`<dataset_version>`.xlxs

Example: seaflow\_2018-05-25\_v1.0.xlxs

`<dataset_short_name>`: short name of the dataset length: less
than 50 characters

`<dataset_release_data>`: date of dataset
release format: %Y-%m-%d note: zero padding required


`<dataset_version>`: associated dataset version length: less than
50 characters


## First Sheet: "data"
Columns by order:

1. **time**: corresponding datetime
	- type: datetime
	- format: %Y-%m-%dT%H:%M:%S [example: 2018-03-29T18:05:55]
	- time zone: UTC
	- note: there is a blank space between date and time
	- note: zero padding required



2. **lat**: latitude
	- type: float
	- format: Decimal (not military grid system)
	- unit: degree north
	- range: [-90, 90]


3. **lon**: longitude
	- type: float
	- format: Decimal (not military grid system)
	- unit: degree east
	- range: [-180, 180]


4. **depth**: depth
    - type: float
    - unit: meters
    - range: [0, ∞]


5. **&lt;v_1&gt;**: first variable (short name)

Add more columns similar to the last column, if dataset has more than one variable. 	



## Second Sheet: "dataset_meta_data"
Columns by order:

1. **dataset_short_name**: dataset short name
    - type: string
    - length: <50 chars
    - short, human readable name of your dataset.
    - example: BATS Bacteria Production

2. **dataset_long_name**: descriptive dataset name
    - type: string
    - length: <500 chars
    - Descriptive human readable name of your dataset
    - example: Bermuda Atlantic Time-series Study (BATS) Bacteria Production

3. **dataset_version**: dataset version
    - type: string
    - length: <50 chars
    - examples: V1, Version 3.5

4. **dataset_release_date**: dataset release date
    - type: date
    - format: %Y-%m-%d (zero padding required)

5. **dataset_make**: how dataset is made (fixed options= [assimilation, model, observation])
    - type: string
    - length: <50 chars

6. **dataset_source**: name of your lab and/or institution
    - type: string
    - length: <100 chars
    - example: Bermuda Institute of Ocean Sciences

7. **official_cruise_name(s)**: If applicable, list official cruise name associated with your dataset. (enter each ref. in a separate row). (optional).
    - type: string
    - example:  KOK1606

8. **dataset_distributor**: name of the distributor of the data product (optional: if the dataset source differs from the distributor).
    - type: string
    - length: <100 chars
    - example:  Distributed by NASA PODAAC

9. **dataset_acknowledgement**: Any acknowledgement(s) for this dataset
  	- type: string
  	- length: <100 chars

10. **contact_email**: Email address of data submitter. Note: This will be public information in the database.
  	- type: string
  	- length: <100 chars


11. **dataset_history**: notes regarding the evolution of the dataset with respect to the previous versions, if applicable.
  	- type: string
  	- length: <100 chars

12. **dataset_description**: A description of your dataset detailing collection and processing methodology.
  	- type: string
  	- length: no limit

13. **dataset_references**: Links/citations associated with the dataset documentations/publications (enter each ref. in a separate row). (optional).
  	- type: string
  	- length: <500 chars per item

14. **climatology**: is the dataset a climatology product? (<null if not climatology, 1 climatology>)
  	- type: string
  	- length: <10 chars



## Third Sheet: "vars_meta_data"
Columns by order:

1. **var_short_name**: variable short name - code friendly: no illegal chars (spaces, !@#$%^etc..)
	- type: string
	- length: <50 chars

2. **var_long_name**: descriptive variable name
	- type: string
	- length: <500 chars


3. **var_unit**: variable unit
	- type: string
	- length: <50 chars

4. **var_sensor**: device by which variable is measured
	- type: string
	- length: <50 chars
	- examples: [satellite, cruise_name, simulation, ...]

5. **var_spatial_res**: variable spatial resolution
	- type: string
	- length: <50 chars
	- examples: [1/25° X 1/25° , 50km X 50km, Irregular, ...]

6. **var_temporal_res**: variable temporal resolution
	- type: string
	- length: <50 chars
	- examples: [Hourly, Daily, Irregular, ...]


7. **var_discipline**: the closest discipline(s) associated with the variable
	- type: string
	- length: <100 chars
	- examples: [Physics, Chemistry, Biology, BioGeoChemistry, ...]		

8. **visualize**: Is this variable visualizable? If not, it can be excluded from the Simons CMAP web application.
	- type: int
	- length: <2 chars
	- examples: [0 is not visualizable, 1 is visualizable]. ex: station # = 0 (non visualize), prochlorococcus abundance = 1 (visualize)


9. **var_keywords**: keywords pertinent to the variable (separated by comma) - These are *extremely* important for others to 	locate your dataset.
	- type: string
	- length: <500 chars
	- examples: [pro, prochloro, prochlorococcus, seaflow, flow, cytometry, flow-cytometry, insitu, in-situ, cruise, observation, rep, reprocessed, bio, biology, armbrust, UW, University of Washington, abundance,cell abundance]

		**Keywords are variable-specific and case-insensitive. Please separate each keyword by comma. The suggested format for each variable keyword list is:**

	  -	Example keywords related to any official or unofficial variable names: pro / prochloro / …
		- Example keywords related to sensor/apparatus: cruise / satellite / computer (in case of mode) / SeaFlow / ….
		- Example keywords related to official or unofficial cruise names (if applicable): KM1427 / Gradients 2.0 / ….
		- Example keywords related to data owners institution: UW / University of Washington / …
		- Example keywords related to data production techniques: cytometry / flow cytometry / …
		- Example keywords related to the research context: omics / 16s / …
		- Example keywords related to the associated discipline(s): chemistry / biology / physics / biogeochemical / biogeography …
		- Any other keywords you think are relevant
		- Keyword Example for <proprochloro_abundance> variable in the SeaFlow Dataset:

			*pro, prochloro, prochlorococcus, seaflow, flow, cytometry, flow-cytometry, insitu, in-situ, cruise, observation, rep, reprocessed, bio, biology, armbrust, UW, University of Washington, abundance,cell abundance*

10. **var_comment**: any other comment about the variable
	- type: string
	- length: no limit		
