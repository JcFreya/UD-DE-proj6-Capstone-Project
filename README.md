# Data Engineering Capstone Project

### Overview
The project will work with four datasets to complete the project. The main dataset will include data on immigration to the United States, and supplementary datasets will include data on airport codes, U.S. city demographics, and temperature data. 

### Datasets

-   **I94 Immigration Data:**  This data comes from the US National Tourism and Trade Office.  [This](https://travel.trade.gov/research/reports/i94/historical/2016.html)  is where the data comes from. There's a sample file so you can take a look at the data in csv format before reading it all in.
-   **World Temperature Data:**  This dataset came from Kaggle. You can read more about it  [here](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data).
-   **U.S. City Demographic Data:**  This data comes from OpenSoft. You can read more about it  [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).
-   **Airport Code Data:**  This is a simple table of airport codes and corresponding cities. It comes from  [here](https://datahub.io/core/airport-codes#data).

### Folder Structure
-   **etl.py**  - reads data from S3, processes that data using Spark, and writes them back to S3
-   **etl_functions.py, utility.py**  - contains the functions for creating fact and dimension tables, data visualizations and cleaning.
-   **config.cfg**  - contains configuration and AWS credentials
-   **capstone-project.ipynb**  - import data, clean and process the data, set up the pipeline

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


