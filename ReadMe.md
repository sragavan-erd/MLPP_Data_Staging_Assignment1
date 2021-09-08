# Data Source

Data sourced from https://census.gov
Data table: American Community Survey 2019

# Variables

B01003_001E - Total Population
B25051_001E - Total Kitchen Facilities
B25041_001E - Total Bedrooms
B01001_002E - Total Males
B01001_026E - Total Females

All metrics are at a Block-Group level granularity


# Functions

ACS5_API - This function uses the census.gov API to  source data as a .json file
get_df - Used to process  the json file and output a dataframe
pgsql - Used to push the dataframe into the sql server







