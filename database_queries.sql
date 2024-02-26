-- 1. How many stores does the business have and in which countries?
SELECT 	country_code AS country, 
        COUNT(country_code) AS total_no_stores
FROM
    dim_store_details
GROUP BY
    country
ORDER BY
    total_no_stores DESC;

-- 2. Which locations currently have the most stores?
SELECT 	locality, 
        COUNT(locality) AS total_no_stores
FROM
    dim_store_details
GROUP BY
    locality
ORDER BY
    total_no_stores DESC
LIMIT
    7;

-- 3. Which months produced the largest amount of sales?
SELECT
    dim_date_times.month AS month_number,
    ROUND(CAST(SUM(dim_products.product_price * orders_table.product_quantity) AS NUMERIC), 2) AS total_sales_amount
FROM
    orders_table
JOIN
    dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
JOIN
    dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY 
    month_number
ORDER BY
    total_sales_amount DESC;

-- 4. How many sales are coming from online?
SELECT
    CASE
        WHEN store_code LIKE 'WEB%' THEN 'Web'
        ELSE 'Offline'
    END AS "location",
    COUNT(*) AS number_of_sales, 
    SUM(product_quantity) AS product_quantity_count
FROM
    orders_table
GROUP BY
    CASE
        WHEN store_code LIKE 'WEB%' THEN 'Web'
        ELSE 'Offline'
    END;

-- 5. What percentage of sales come through each type of store?
SELECT
    dim_store_details.store_type,
    ROUND(CAST(SUM(dim_products.product_price * orders_table.product_quantity) AS NUMERIC), 2) AS total_sales,
    ROUND((COUNT(orders_table.store_code) * 100.0 / (SELECT COUNT(*) FROM orders_table)), 2) AS percentage_total
FROM 
    orders_table
JOIN 
    dim_products ON orders_table.product_code = dim_products.product_code
JOIN
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY
    dim_store_details.store_type
ORDER BY
    percentage_total DESC;

-- 6. Which month in each year produced the highest cost of sales?
WITH monthly_sales AS (
    SELECT
        dim_date_times.year AS year,
        dim_date_times.month AS month,
        ROUND(CAST(SUM(dim_products.product_price * orders_table.product_quantity) AS NUMERIC), 2) AS total_sales_amount,
        RANK() OVER (PARTITION BY dim_date_times.year ORDER BY SUM(dim_products.product_price * orders_table.product_quantity) DESC) AS sales_rank
    FROM
        orders_table
    JOIN
        dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
    JOIN
        dim_products ON orders_table.product_code = dim_products.product_code
    GROUP BY 
        dim_date_times.year, dim_date_times.month
)
SELECT
    year,
    month,
    total_sales_amount
FROM
    monthly_sales
WHERE
    sales_rank = 1
ORDER BY
    total_sales_amount DESC;

-- 7. What is our staff headcount?
SELECT 
    SUM(staff_numbers) AS total_staff_numbers,
    country_code 
FROM 
    dim_store_details
GROUP BY
    country_code
ORDER BY
    total_staff_numbers DESC;

-- 8. Which German store type is selling the most?
SELECT
    dim_store_details.store_type,
    dim_store_details.country_code,
    ROUND(CAST(SUM(dim_products.product_price * orders_table.product_quantity) AS NUMERIC), 2) AS total_sales
FROM 
    orders_table
JOIN 
    dim_products ON orders_table.product_code = dim_products.product_code
JOIN
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
WHERE
    dim_store_details.country_code = 'DE'
GROUP BY
    dim_store_details.store_type, dim_store_details.country_code
ORDER BY 
    total_sales DESC;

-- 9. How quickly is the company making sales?
WITH event_time_column AS (
    SELECT
        TO_TIMESTAMP(CONCAT(year || '-' || month || '-' || day || ' ' || timestamp), 'YYYY-MM-DD HH24:MI:SS') AS event_time
    FROM	
        dim_date_times
),
next_time AS (
    SELECT
        event_time,
        LEAD(event_time) OVER (ORDER BY event_time) AS next_event_time
    FROM
        event_time_column
), 
time_difference AS (
    SELECT
        event_time,
        next_event_time,
        (next_event_time - event_time) AS time_difference_interval
    FROM
        next_time
)

SELECT
    EXTRACT(YEAR FROM event_time) AS year,
    CONCAT(
        '{"hours": ', EXTRACT(HOUR FROM AVG(time_difference_interval)),
        ', "minutes": ', EXTRACT(MINUTE FROM AVG(time_difference_interval)),
        ', "seconds": ', EXTRACT(SECOND FROM AVG(time_difference_interval)),
        ', "milliseconds": ', EXTRACT(MILLISECONDS FROM AVG(time_difference_interval)), '}'
    ) AS actual_time_taken
FROM 
    time_difference
GROUP BY
    year
ORDER BY 
    AVG(time_difference_interval) DESC;
