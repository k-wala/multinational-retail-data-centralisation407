-- Statement to change the data types of columns in the orders table
ALTER TABLE orders_table
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,                
    ALTER COLUMN card_number TYPE VARCHAR(19), 
    ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN product_code TYPE VARCHAR(11), 
    ALTER COLUMN product_quantity TYPE SMALLINT;

-- Statement to change the data types of columns in the users table
ALTER TABLE dim_users
    ALTER COLUMN first_name TYPE VARCHAR(255),
    ALTER COLUMN last_name TYPE VARCHAR(255),                
    ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::date, 
    ALTER COLUMN country_code TYPE VARCHAR(3),
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID, 
    ALTER COLUMN join_date TYPE DATE USING join_date::date;

-- Statement to change the data types of columns in the store details table
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

-- Statement to create new column based on weight ranges
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

-- Statement to alter column types for the dim_products table
ALTER TABLE dim_products
    ALTER COLUMN product_price TYPE FLOAT USING product_price::double precision,
    ALTER COLUMN "EAN" TYPE VARCHAR(17),
    ALTER COLUMN product_code TYPE VARCHAR(11),
    ALTER COLUMN date_added TYPE DATE USING date_added::date,
    ALTER COLUMN uuid TYPE UUID USING uuid::UUID,
    ALTER COLUMN still_available TYPE BOOL USING CASE WHEN still_available = 'Still_avaliable' THEN TRUE ELSE FALSE END;


-- Statement to alter column types for date times table
ALTER TABLE dim_date_times
    ALTER COLUMN month TYPE VARCHAR(2),
    ALTER COLUMN year TYPE VARCHAR(4),
    ALTER COLUMN day TYPE VARCHAR(2),
    ALTER COLUMN time_period TYPE VARCHAR(10),
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;


-- Statement to alter column types for card details table
ALTER TABLE dim_card_details
    ALTER COLUMN card_number TYPE VARCHAR(20),
    ALTER COLUMN expiry_date TYPE VARCHAR(5),
    ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::date;
