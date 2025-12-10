-- Sample Data for AI Pivot Studio Demo
-- Run this against your PostgreSQL database

-- Create dimension tables
CREATE TABLE IF NOT EXISTS dim_date (
    id SERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    quarter VARCHAR(2),
    month VARCHAR(20),
    day INTEGER
);

CREATE TABLE IF NOT EXISTS dim_product (
    id SERIAL PRIMARY KEY,
    category VARCHAR(50),
    subcategory VARCHAR(50),
    product_name VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_region (
    id SERIAL PRIMARY KEY,
    country VARCHAR(50),
    state VARCHAR(50),
    city VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_customer (
    id SERIAL PRIMARY KEY,
    segment VARCHAR(50),
    customer_name VARCHAR(100)
);

-- Create fact table
CREATE TABLE IF NOT EXISTS fact_sales (
    id SERIAL PRIMARY KEY,
    date_id INTEGER REFERENCES dim_date(id),
    product_id INTEGER REFERENCES dim_product(id),
    region_id INTEGER REFERENCES dim_region(id),
    customer_id INTEGER REFERENCES dim_customer(id),
    order_id VARCHAR(20),
    sales_amount DECIMAL(12,2),
    quantity INTEGER,
    profit DECIMAL(12,2),
    discount DECIMAL(5,4)
);

-- Insert sample dimension data
INSERT INTO dim_date (year, quarter, month, day) VALUES
(2023, 'Q1', 'January', 1),
(2023, 'Q1', 'February', 1),
(2023, 'Q1', 'March', 1),
(2023, 'Q2', 'April', 1),
(2023, 'Q2', 'May', 1),
(2023, 'Q2', 'June', 1),
(2023, 'Q3', 'July', 1),
(2023, 'Q3', 'August', 1),
(2023, 'Q3', 'September', 1),
(2023, 'Q4', 'October', 1),
(2023, 'Q4', 'November', 1),
(2023, 'Q4', 'December', 1),
(2024, 'Q1', 'January', 1),
(2024, 'Q1', 'February', 1),
(2024, 'Q1', 'March', 1),
(2024, 'Q2', 'April', 1),
(2024, 'Q2', 'May', 1),
(2024, 'Q2', 'June', 1);

INSERT INTO dim_product (category, subcategory, product_name) VALUES
('Electronics', 'Phones', 'iPhone 15'),
('Electronics', 'Phones', 'Samsung Galaxy S24'),
('Electronics', 'Laptops', 'MacBook Pro'),
('Electronics', 'Laptops', 'Dell XPS'),
('Electronics', 'Tablets', 'iPad Pro'),
('Furniture', 'Chairs', 'Office Chair'),
('Furniture', 'Desks', 'Standing Desk'),
('Furniture', 'Storage', 'Filing Cabinet'),
('Office Supplies', 'Paper', 'Copy Paper'),
('Office Supplies', 'Pens', 'Ballpoint Pens');

INSERT INTO dim_region (country, state, city) VALUES
('USA', 'California', 'Los Angeles'),
('USA', 'California', 'San Francisco'),
('USA', 'New York', 'New York City'),
('USA', 'Texas', 'Houston'),
('USA', 'Texas', 'Austin'),
('Canada', 'Ontario', 'Toronto'),
('Canada', 'British Columbia', 'Vancouver'),
('UK', 'England', 'London'),
('Germany', 'Bavaria', 'Munich'),
('Japan', 'Tokyo', 'Tokyo');

INSERT INTO dim_customer (segment, customer_name) VALUES
('Consumer', 'John Smith'),
('Consumer', 'Jane Doe'),
('Corporate', 'Acme Corp'),
('Corporate', 'Tech Solutions'),
('Home Office', 'Bob Wilson'),
('Small Business', 'Local Shop'),
('Enterprise', 'Big Company Inc');

-- Insert sample fact data
INSERT INTO fact_sales (date_id, product_id, region_id, customer_id, order_id, sales_amount, quantity, profit, discount)
SELECT 
    (random() * 17 + 1)::int as date_id,
    (random() * 9 + 1)::int as product_id,
    (random() * 9 + 1)::int as region_id,
    (random() * 6 + 1)::int as customer_id,
    'ORD-' || LPAD((row_number() over())::text, 6, '0') as order_id,
    (random() * 5000 + 100)::decimal(12,2) as sales_amount,
    (random() * 20 + 1)::int as quantity,
    (random() * 500 + 10)::decimal(12,2) as profit,
    (random() * 0.3)::decimal(5,4) as discount
FROM generate_series(1, 500);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON fact_sales(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON fact_sales(product_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_region ON fact_sales(region_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON fact_sales(customer_id);

