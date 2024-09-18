-- Create a database
CREATE DATABASE designer_products;

-- Connect to the new database
\connect designer_products;


-- Create a table in the designer_products database
CREATE TABLE products (
    product_id VARCHAR(50) PRIMARY KEY,
    last_scrape_date DATE,
    last_price DECIMAL(10, 2)
);



