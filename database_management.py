from sqlalchemy import create_engine
from sqlalchemy import text
from main import db_creds

# Creating engine to connect to the sales_data database in PostgreSQL
engine = create_engine(f"postgresql://{db_creds['user']}:{db_creds['password']}@{db_creds['host']}:{db_creds['port']}/{db_creds['database']}")

# Define the alter statement to change the data types of columns in the orders table
orders_table_new_columns = """
    ALTER TABLE orders_table
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,                
    ALTER COLUMN card_number TYPE VARCHAR(19), 
    ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN product_code TYPE VARCHAR(11), 
    ALTER COLUMN product_quantity TYPE SMALLINT;
"""

# Execute the orders table new columns statement
with engine.connect() as connection:
    connection.execute(text(orders_table_new_columns))
    connection.commit()


# Define the alter statement to change the data types of columns in the users table
users_table_new_columns = """
    ALTER TABLE dim_users
    ALTER COLUMN first_name TYPE VARCHAR(255),
    ALTER COLUMN last_name TYPE VARCHAR(255),                
    ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::date, 
    ALTER COLUMN country_code TYPE VARCHAR(3),
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID, 
    ALTER COLUMN join_date TYPE DATE USING join_date::date;
"""

# Execute users table new columns statement
with engine.connect() as connection:
    connection.execute(text(users_table_new_columns))
    connection.commit()

# Define the alter statement to change the data types of columns in the store details table
store_details_new_columns = """
    ALTER TABLE dim_store_details
    ALTER COLUMN longitude TYPE FLOAT USING longitude::double precision,
    ALTER COLUMN locality TYPE VARCHAR(255),                
    ALTER COLUMN store_code TYPE VARCHAR(12), 
    ALTER COLUMN staff_numbers TYPE SMALLINT,
    ALTER COLUMN opening_date TYPE DATE,
    ALTER COLUMN store_type TYPE VARCHAR(255),
    ALTER COLUMN latitude TYPE FLOAT USING latitude::double precision,
    ALTER COLUMN country_code TYPE VARCHAR(3),
    ALTER COLUMN continent TYPE VARCHAR(255);
"""

# Execute store details table new columns statement
with engine.connect() as connection:
    connection.execute(text(store_details_new_columns))
    connection.commit()

# Define statement to create new column based on weight ranges
new_weight_class_column = """
    ALTER TABLE dim_products
    ADD COLUMN weight_class VARCHAR(15);

    UPDATE dim_products
    SET weight_class = 
        CASE
            WHEN weight < 2 THEN 'Light'
            WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
            WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
            WHEN weight >= 140 THEN 'Truck_Required'
            ELSE 'Unknown'
        END;
"""

# Execute and commit new weight class column
with engine.connect() as connection:
    connection.execute(text(new_weight_class_column))
    connection.commit()

products_table_new_columns = """
    ALTER TABLE dim_products
        ALTER COLUMN product_price TYPE FLOAT USING product_price::double precision,
        ALTER COLUMN "EAN" TYPE VARCHAR(17),
        ALTER COLUMN product_code TYPE VARCHAR(11),
        ALTER COLUMN date_added TYPE DATE USING date_added::date,
        ALTER COLUMN uuid TYPE UUID USING uuid::UUID,
        ALTER COLUMN still_available TYPE BOOL USING CASE WHEN still_available = 'Still_avaliable' THEN TRUE ELSE FALSE END;
"""

# Execute and commit new data types for products table
with engine.connect() as connection:
    connection.execute(text(products_table_new_columns))
    connection.commit()

# Define statement to alter column types for date times table
date_times_table_new_columns = """
    ALTER TABLE dim_date_times
        ALTER COLUMN month TYPE VARCHAR(2),
        ALTER COLUMN year TYPE VARCHAR(4),
        ALTER COLUMN day TYPE VARCHAR(2),
        ALTER COLUMN time_period TYPE VARCHAR(10),
        ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;
"""

# Execute and commit new data types for date times table
with engine.connect() as connection:
    connection.execute(text(date_times_table_new_columns))
    connection.commit()


# Define statement to alter column types for card details table
card_details_new_columns = """
    ALTER TABLE dim_card_details
        ALTER COLUMN card_number TYPE VARCHAR(20),
        ALTER COLUMN expiry_date TYPE VARCHAR(5),
        ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::date;
"""

# Execute and commit new data types for card details table
with engine.connect() as connection:
    connection.execute(text(card_details_new_columns))
    connection.commit()