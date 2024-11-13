-- Create a database
CREATE DATABASE designer_products;

-- Connect to the new database
\connect designer_products;


-- Create a table in the designer_products database
CREATE TABLE products (
    product_id VARCHAR(50) PRIMARY KEY,
    brand VARCHAR(50),
    product_name VARCHAR(150),
    curr_price DECIMAL(10, 2),
    curr_scrape_date DATE,
    prev_price DECIMAL(10, 2),
    prev_scrape_date DATE,
    sold_date DATE,
    sold BOOLEAN,
    listing_url VARCHAR(255),
    source VARCHAR(50)
);

-- Create users table
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(120) UNIQUE NOT NULL,
    password_hash VARCHAR(256) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP WITH TIME ZONE,
    is_active BOOLEAN DEFAULT TRUE
);
