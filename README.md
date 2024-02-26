# Multinational Retail Data Centralisation

The aim of this project was to centralise the data infrastructure of a global retail company so all their data can be accessed from one location - a PostgreSQL database. 

## Technologies Used
- Python
- Pandas
- Requests
- tabula-py
- SQLAlchemy
- psycopg2
- Boto3
- PyYAML


## File Structure

1. Data extraction - the data_extraction.py file includes all methods for extracting the company's data from different sources: Amazon RDS, PDF files, a RESTful API and Amazon S3 buckets.  
2. Data cleaning - once the data has been extracted, it's then turned into a Pandas dataframe and cleaned in the data_cleaning.py file. Null and extraneous values are removed and data is formatted. 
3. Data uploading - using the database_utils.py file, the cleaned dataframes are then uploaded as tables to the PostgreSQL database.
4. Database management - in the database_management.sql file the star-based schema is created. Table columns are cast to the correct data types, and the primary and foreign keys are created to link all the tables.
5. Database queries - the database_queries.sql file contains all the data queries providing up to date metrics for the company. 
6. main.py - contains the main logic of the project & imports all necessary modules and packages. 