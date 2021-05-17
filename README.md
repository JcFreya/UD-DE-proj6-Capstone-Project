# Data Engineering Capstone Project

### Overview
The project will work with four datasets to complete the project. The main dataset will include data on immigration to the United States, and supplementary datasets will include data on airport codes, U.S. city demographics, and temperature data. 

### Datasets

-   **I94 Immigration Data:**  This data comes from the US National Tourism and Trade Office.  [This](https://travel.trade.gov/research/reports/i94/historical/2016.html)  is where the data comes from. There's a sample file so you can take a look at the data in csv format before reading it all in.
	- i94yr - 4 digit year 
	- i94mon - Numeric month 
	- i94cit - 3 digit code for immigrant country of birth
	- i94res - 3 digit code for immigrant country of residence
	- i94port - Port of admission 
	- arrdate - Arrival Date in the USA. It is a SAS date numeric field that a 
   permament format has not been applied. 
	- i94mode - Mode if transportation. (1 = 'Air', 2 = 'Sea', 3 = 'Land', 9 = 'Not reported')
	- i94addr - USA State of arrival. There is lots of invalid codes in this variable and the list in distionary file shows what we have found to be valid, everything else goes into 'other' 
	- depdate - Departure Date from the USA. It is a SAS date numeric field that 
   a permament format has not been applied. 
	- i94bir - Age of Respondent in Years 
	- i94visa - Visa codes collapsed into three categories (1 = Business, 2 = Pleasure, 3 = Student)
	- count - Used for summary statistics 
	- dtadfile - Character Date Field - Date added to I-94 Files - CIC does not use 
	- visapost - Department of State where where Visa was issued - CIC does not use 
	- occup - Occupation that will be performed in U.S. - CIC does not use 
	- entdepa - Arrival Flag - admitted or paroled into the U.S. - CIC does not use 
	- entdepd - Departure Flag - Departed, lost I-94 or is deceased - CIC does not use 
	- entdepu - Update Flag - Either apprehended, overstayed, adjusted to perm residence - CIC does not use 
	- matflag - Match flag - Match of arrival and departure records 
	- biryear - 4 digit year of birth 
	- ataddto - Character Date Field - Date to which admitted to U.S. (allowed to stay until) - CIC does not use 
	- gender - Non-immigrant sex 
	- insnum - INS number 
	- airline - Airline used to arrive in U.S. 
	- admnum - Admission Number 
	- fltno - Flight number of Airline used to arrive in U.S. 
	- visatype - Class of admission legally admitting the non-immigrant to temporarily stay in U.S.
-   **World Temperature Data:**  This dataset came from Kaggle. You can read more about it  [here](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data). The data contains the information of global average temperature, average temperature uncertainty by country and city.
	- dt - Date
	- AverageTemperature - Global average land temperature in celsius
	- AverageTemperatureUncertainty - 95% confidence interval around the average
	- City - Name of City
	- Country - Name of Country
	- Latitude - City Latitude
	- Longitude - City Longitude
-   **U.S. City Demographic Data:**  This data comes from OpenSoft. You can read more about it  [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).
	- City - City Name
	- State - US State where city is located
	- Median Age - Median age of the population
	- Male Population - Count of male population
	- Female Population - Count of female population
	- Total Population - Count of total population
	- Number of Veterans - Count of total Veterans
	- Foreign born - Count of residents of the city that were not born in the city
	- Average Household Size - Average city household size
	- State Code - Code of the US state
	- Race - Respondent race
	- Count - Count of city's individual per race
-   **Airport Code Data:**  This is a simple table of airport codes and corresponding cities. It comes from  [here](https://datahub.io/core/airport-codes#data).
	- ident - 
	- type
	- name
	- elevation_ft
	- continent
	- iso_country
	- iso_region
	- municipality
	- gps_code
	- iata_code
	- local_code
	- coordinates
### Folder Structure
-   **etl.py**  - reads data from S3, processes that data using Spark, and writes them back to S3
-   **etl_utils.py, utils.py**  - contains the functions for creating fact and dimension tables, data visualizations and cleaning.
-   **config.cfg**  - contains configuration and AWS credentials
-   **capstone-project.ipynb**  - import data, clean and process the data based on basic EDA, design the pipeline

### Project steps as following:

#### Step 1: Scope the Project and Gather Data

Since the scope of the project will be highly dependent on the data, these two things happen simultaneously. In this step:

-   Identify and gather the data that will be used for the project
-   Explain what end use cases to prepare the data for

#### Step 2: Explore and Assess the Data

-   Explore the data to identify data quality issues, like missing values, duplicate data, etc.
-   Document steps necessary to clean the data

#### Step 3: Define the Data Model

-   Map out the conceptual data model and explain the reason of choosing that model
-   List the steps necessary to pipeline the data into the chosen data model

#### Step 4: Run ETL to Model the Data

-   Create the data pipelines and the data model
-   Include a data dictionary
-   Run data quality checks to ensure the pipeline ran as expected
    -   Integrity constraints on the relational database (e.g., unique key, data type, etc.)
    -   Unit tests for the scripts to ensure they are doing the right thing
    -   Source/count checks to ensure completeness

#### Step 5: Complete Project Write Up

-   The Project Write Up includes the topic and discussion on 'What's the goal? What queries will be used? How would Spark or Airflow be incorporated? Why chose the model?
-   It will also clearly state the rationale for the choice of tools and technologies for the project.
-   Document the steps of the process.
-   Propose how often the data should be updated and why.
-   Include a description of how to approach the problem differently under the following scenarios:
    -   If the data was increased by 100x.
    -   If the pipelines were run on a daily basis by 7am.
    -   If the database needed to be accessed by 100+ people.

### Run the ETL pipeline
The ETL pipeline is located in the etl.py script

