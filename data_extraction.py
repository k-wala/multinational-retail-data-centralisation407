import pandas as pd
import tabula
import requests
import boto3

class DataExtractor:
    """This class extracts data from various sources."""

    def __init__(self):
        """Initialising the class."""

        self.engine = None # Database engine
        self.table_names = None # List of table names

    def read_rds_table(self, db_connector, specific_table = None):
        """
        Reads tables from an RDS database and returns a Dataframe.
        
        Parameters:
        - db_connector: Database connector object.
        - specific_table: Optional parameter. If provided, it only reads the specified table.
        
        Returns:
        - dictionary of dataframes if specific_table is None.
        - Dataframe for the specified table if specific_table is provided.
        """
        if self.engine is None:
            self.engine = db_connector.init_db_engine()
        if self.table_names is None:
            self.table_names = db_connector.list_db_tables()
        
        all_dataframes = {}

        for table_name in self.table_names:
            if specific_table and table_name != specific_table:
                continue
            df_table = pd.read_sql_table(table_name, self.engine)
            all_dataframes[table_name] = df_table
        
        if specific_table:
            if specific_table and not all_dataframes:
                return f"{specific_table} table not found."
            return all_dataframes.get(specific_table, pd.DataFrame())
        else:
            return all_dataframes
    
    def retrieve_pdf_data(self, pdf_link):
        """
        Reads data from a PDF and returns a Dataframe.
        
        Parameters:
        - pdf_link: link to the PDF.
        
        Returns:
        - Dataframe of the PDF data,
        """
        df_list = tabula.read_pdf(pdf_link, pages='all')
        merged_pdf_df = pd.concat(df_list)
        return merged_pdf_df
    
    def list_number_of_stores(self, endpoint, header_dict):
        """
        Retrieves the number of stores from the specified API endpoint.
        
        Parameters:
        - endpoint: API endpoint URL.
        - header_dict: dictionary containing the request headers.
        
        Returns:
        The number of stores retrieved from the API.
        """

        response = requests.get(endpoint, headers=header_dict)
        json_data = response.json()
        return json_data['number_stores']
    
    def retrieve_stores_data(self):
        """
        Retrieves store data from multiple store detail endpoints.

        This method iterates over a range of store numbers and makes requests to
        individual store detail endpoints to retrieve store data. The retrieved
        data is aggregated into a DataFrame containing store information.
        
        Returns:
        - Dataframe containing the store data.
        """

        store_data_list = []
        num_of_stores = 451

        for store_number in range(num_of_stores):

            stores_endpoint = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
            header = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
            store_data = requests.get(stores_endpoint, headers=header)

            if store_data.status_code == 200:
                json_data = store_data.json()
                store_data_list.append(json_data)
            else:
                print(f"Failed to retrieve data for Store Number: {store_number}. Status code: {store_data.status_code}")

        stores_df = pd.DataFrame(store_data_list)
        return stores_df
    
    def extract_from_s3(self, s3_address):
        """
        Extracts product data from a CSV file stored in S3.
        
        Parameters:
        - s3_address: the address of the CSV in S3.
        
        Returns:
        - Dataframe containing product data extracted from the CSV.
        """
        bucket_name = s3_address.split('//')[1].split('/')[0]
        object_key = s3_address.split('//')[1].split('/')[1]

        s3 = boto3.client('s3')

        response = s3.get_object(Bucket=bucket_name, Key=object_key)

        products_df = pd.read_csv(response['Body'])
        return products_df
    
    def extract_json_from_s3(self, s3_json_address):
        """
        Extracts date times data from a JSON file stored in S3.
        
        Parameters:
        - s3_json_address: address of the JSON file in S3.
        
        Returns:
        - Dataframe containing date time data extracted from the JSON.
        """
        bucket_name = s3_json_address.split('/')[2].split('.')[0]
        object_key = s3_json_address.split('/')[3]

        s3 = boto3.client('s3')

        response = s3.get_object(Bucket=bucket_name, Key=object_key)

        dates_df = pd.read_json(response['Body'])
        return dates_df


    