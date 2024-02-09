import pandas as pd

class DataCleaning:
    """This class contains methods to clean various different Pandas Dataframes."""

    def __init__(self):
        pass

    def clean_user_data(self):
        """
        Cleans the user data by performing various different data cleaning operations.
        
        Returns:
        - Dataframe: cleaned user data dataframe.
        """

        # Read user data from the database
        from main import db_connector, data_extractor
        users_df = data_extractor.read_rds_table(db_connector, specific_table = "legacy_users")

        # Removing any duplicate rows
        users_df = users_df.drop_duplicates() 

        # Removing the 'index' column so only one remains 
        users_df = users_df.drop('index', axis=1) 

        # Removing all rows where the string "NULL" is a value
        null_names = users_df[users_df['first_name'] == "NULL"].index 
        users_df = users_df.drop(null_names) 

        # Removing all rows where first_name contains a number
        numbers_mask = users_df['first_name'].str.contains('\d')
        users_df = users_df.drop(users_df[numbers_mask].index) 

        # Replacing all newline characters in the address column with commas
        users_df['address'] = users_df['address'].str.replace('\n', ', ')

        # Removing all rows with NaN values
        users_df = users_df.dropna()

        # Resetting the index after row removals 
        users_df = users_df.reset_index(drop=True)

        return users_df
    
    def clean_card_data(self, data_extractor, pdf_link):
        """
        Cleans the card data by performing various different data cleaning operations.

        Parameters:
        - data_extractor: instance of the data extractor class
        - pdf_link: link to the pdf that the data will be extracted from
        
        Returns:
        - Dataframe: cleaned card data dataframe.
        """

        # Extract card data from the pdf link
        card_data_df = data_extractor.retrieve_pdf_data(pdf_link)

        # Removing any duplicate rows
        card_data_df = card_data_df.drop_duplicates() 
        card_data_df = card_data_df.reset_index(drop=True)

        # Removing all rows with anomaly values e.g. "NULL" strings and alphanumeric strings
        anomaly_values = card_data_df[~card_data_df['expiry_date'].str.match(r'\d{2}/\d{2}')]
        card_data_df = card_data_df.drop(anomaly_values.index)
        card_data_df = card_data_df.reset_index(drop=True)

        # Removing all rows with NaN / Null / NaT values
        card_data_df = card_data_df.dropna()
        card_data_df = card_data_df.reset_index(drop=True)

        # Replace '?' with an empty string only in rows where 'card_number' contains a question mark
        card_data_df['card_number'] = card_data_df['card_number'].astype(str)
        card_data_df['card_number'] = card_data_df['card_number'].apply(lambda x: x.lstrip('?'))
        
        # Resetting the index after all row removals
        card_data_df = card_data_df.reset_index(drop=True)

        return card_data_df
    
    def clean_store_data(self, data_extractor):
        """
        Cleans the store data by performing various different data cleaning operations.

        Parameters:
        - data_extractor: instance of the data extractor class
        
        Returns:
        - Dataframe: cleaned store data dataframe.
        """

        # Extracting the store data from the API endpoint
        store_data_df = data_extractor.retrieve_stores_data()

        # Removing any duplicate rows
        store_data_df = store_data_df.drop_duplicates() 

        # Removing the 'index' column so only one remains 
        store_data_df = store_data_df.drop('index', axis=1) 

        # Removing the 'lat' column 
        store_data_df = store_data_df.drop('lat', axis=1) 

        # Identifying and removing rows with anomaly values e.g. "NULL" strings and alphanumeric strings
        anomaly_values = store_data_df[store_data_df['country_code'].str.len() != 2].index
        store_data_df = store_data_df.drop(anomaly_values)

        # Removing letters from values which contain letters in the 'staff_numbers' column 
        store_data_df['staff_numbers'] = store_data_df['staff_numbers'].str.replace('[^0-9]', '', regex=True)

        # Converting 'staff_numbers' to int
        store_data_df['staff_numbers'] = pd.to_numeric(store_data_df['staff_numbers'], errors='coerce', downcast='integer')

        # Replacing newline characters with commas in 'address' column
        store_data_df['address'] = store_data_df['address'].str.replace('\n', ', ')

        # Converting 'opening_date' column to datetime
        store_data_df['opening_date'] = pd.to_datetime(store_data_df['opening_date'], errors='coerce')

        # Removing the 'ee' at the start of some of the continent values
        store_data_df['continent'] = store_data_df['continent'].str.replace('ee', '')

        # Rearranging order of columns 
        new_column_order = ['store_code', 'store_type', 'staff_numbers', 'opening_date', 'address', 'locality', 'country_code', 'continent', 'longitude', 'latitude']
        store_data_df = store_data_df[new_column_order]

        # Resetting the index after all modifications
        store_data_df = store_data_df.reset_index(drop=True)

        return store_data_df
    
    def convert_product_weights(self, df_name):
        """
        Converts weights in various units into kilograms and updates the dataframe.
        
        Parameters:
        - df_name: dataframe containing the weight column to be converted. 
        
        Returns:
        - dataframe: updated dataframe with all weight values converted to kg. 
        """

        def convert_to_kg(value):
            """
            Converts each weight value to kg. 
            
            Parameters:
            - value: the weight value to convert.
            
            Returns:
            - str or None: the converted weight in kilograms with 'kg' appended, or None if conversion failed.
            """

            # Converting value to lowercase and strip any leading/trailing whitespace
            value = str(value).lower()
            value = value.strip()
            
            # Dictionary of conversion factors for different units 
            conversions = {'g': 0.001, 'ml': 0.001, 'oz': 0.028, 'kg': 1.0}

            if pd.isna(value):
                    return value

            # If statement to handle compound values like '2 x 500g'
            if 'x' in value:
                parts = value.split('x')
                try:
                    quantity = float(parts[0].strip())
                    weight = parts[1].strip()
                    if weight.endswith('g'):
                        weight = weight[:-1]
                        final_weight_1 = quantity * float(weight) * conversions.get('g')
                        return f"{round(final_weight_1, 2)}kg"
                except (ValueError, IndexError):
                    return None
            else:
                numeric_part = ''
                unit = ''
                for char in value:
                    if char.isdigit() or char == '.':
                        numeric_part += char
                    else:
                        unit += char

                if not unit:
                    unit = 'kg'

                if numeric_part:  
                    numeric_value = float(numeric_part)
                else:
                    numeric_value = 0.0  
                    
                if unit in conversions:
                    final_weight = numeric_value * conversions[unit]
                    if isinstance(final_weight, float):
                        final_weight_with_unit = f"{round(final_weight, 2)}kg"
                        return final_weight_with_unit
                    else:
                        return final_weight
                else:
                    return None
        
        # Applying conversion function to 'weight' column of dataframe
        df_name['weight'] = df_name['weight'].apply(convert_to_kg)
        return df_name
    
    def clean_products_data(self, products_df):
        """
        Cleans the products data by performing various different data cleaning operations.

        Parameters:
        - products_df: dataframe of products data that needs cleaning
        
        Returns:
        - Dataframe: cleaned products data dataframe.
        """

        # Removing all Null / None values
        products_df = products_df.dropna()

        # Removing any duplicate rows
        products_df = products_df.drop_duplicates() 

        # Removing the 'Unnamed: 0' column
        products_df = products_df.drop('Unnamed: 0', axis=1) 

        # Identifying and removing rows in which the 'category' column contains a number
        numbers_mask = products_df['category'].str.contains('\d')
        products_df = products_df.drop(products_df[numbers_mask].index) 

        # Resetting the index after all modifications
        products_df = products_df.reset_index(drop=True)

        return products_df
    
    def clean_orders_table(self, data_extractor, db_connector):
        """
        Cleans the products data by performing various different data cleaning operations.

        Parameters:
        - data_extractor: instance of data extraction class 
        - db_connector: instance of database connector class
        
        Returns:
        - Dataframe: cleaned orders table dataframe.
        """
        
        # Extracting the orders table dataframe from RDS
        orders_df = data_extractor.read_rds_table(db_connector, specific_table='orders_table')

        # Dropping unnecessary columns
        columns_to_drop = ['level_0', 'index', 'first_name', 'last_name', '1']
        orders_df = orders_df.drop(columns=columns_to_drop, axis=1)

        return orders_df

    def clean_sales_data(self, data_extractor, s3_json_address):
        """
        Cleans the sales data by performing various different data cleaning operations.

        Parameters:
        - data_extractor: instance of data extraction class 
        - s3_json_address: URL to the JSON file to extract data from
        
        Returns:
        - Dataframe: cleaned sales table dataframe.
        """
        
        # Extracting the sales data from S3 and converting it to a dataframe
        sales_data_df = data_extractor.extract_json_from_s3(s3_json_address)

        # Removing duplicate rows
        sales_data_df = sales_data_df.drop_duplicates() 

        # Removing rows with anomaly values 
        anomaly_values = sales_data_df['time_period'].str.contains('\d')
        sales_data_df = sales_data_df.drop(sales_data_df[anomaly_values].index) 

        # Removing all rows where the string "NULL" is a value
        null_strings = sales_data_df[sales_data_df['time_period'] == "NULL"].index 
        sales_data_df = sales_data_df.drop(null_strings) 

        # Resetting the index after all modifications
        sales_data_df = sales_data_df.reset_index(drop=True)

        return sales_data_df










