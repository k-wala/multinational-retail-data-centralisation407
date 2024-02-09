from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
import pandas as pd

# Create an instance of DatabaseConnector
db_connector = DatabaseConnector()

# Create an instance of DataExtractor
data_extractor = DataExtractor()

# Creating instance of DataCleaning
data_clean = DataCleaning()

# URL to card details pdf
pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"

# Database credentials for the upload_to_db method in DatabaseConnector
db_creds = {'user' : 'postgres',
            'password' : 'arlo8!!Freya',
            'host' : 'localhost',
            'port' : '5432',
            'database' : 'sales_data'}

# Code uploading card details PDF to Postgres database 
# card_data_df = data_clean.clean_card_data(data_extractor, pdf_link)
# table_name = "dim_card_details"
# db_connector.upload_to_db(card_data_df, table_name, db_creds)

# Cleaning and uploading user data to Postgres
cleaned_user_df = data_clean.clean_user_data()
user_table_name = "dim_users"
db_connector.upload_to_db(cleaned_user_df, user_table_name, db_creds)

# # Code to list number of stores from API
# header = {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
# num_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
# store_number = data_extractor.list_number_of_stores(num_stores_endpoint, header)

# # # Uploading store details dataframe to Postgres database
# store_details_endpoint = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number-1}'
# store_data_df = data_clean.clean_store_data(data_extractor)
# print(store_data_df)
# store_table_name = "dim_store_details"
# db_connector.upload_to_db(store_data_df, store_table_name, db_creds)

# # Extracting, cleaning & uploading the products data to Postgres
# s3_address =  's3://data-handling-public/products.csv'
# products_df = data_extractor.extract_from_s3(s3_address)
# converted_weight_df = data_clean.convert_product_weights(products_df)
# cleaned_products_df = data_clean.clean_products_data(converted_weight_df)
# products_table_name = "dim_products"
# db_connector.upload_to_db(cleaned_products_df, products_table_name, db_creds)

# # Extracting, cleaning & uploading the orders data to Postgres
# orders_df = data_extractor.read_rds_table(db_connector, specific_table='orders_table')
# cleaned_orders_df = data_clean.clean_orders_table(data_extractor, db_connector)
# orders_table_name = 'orders_table'
# db_connector.upload_to_db(cleaned_orders_df, orders_table_name, db_creds)

# # Extracting, cleaning & uploading the date events data to Postgres
# s3_json_address = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
# cleaned_sales_df = data_clean.clean_sales_data(data_extractor, s3_json_address)
# sales_table_name = 'dim_date_times'
# db_connector.upload_to_db(cleaned_sales_df, sales_table_name, db_creds)
