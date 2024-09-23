-- Create a database
CREATE DATABASE designer_products;

-- Connect to the new database
\connect designer_products;


-- Create a table in the designer_products database
CREATE TABLE products (
    product_id VARCHAR(50) PRIMARY KEY,
    brand VARCHAR(50),
    product_name VARCHAR(50),
    scrape_date DATE,
    curr_price DECIMAL(10, 2),
    last_scrape_date DATE,
    prev_price DECIMAL(10, 2)
);



