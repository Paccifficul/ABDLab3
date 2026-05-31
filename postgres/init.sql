CREATE TABLE IF NOT EXISTS dim_date (
    date_key DATE PRIMARY KEY,
    day SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    quarter SMALLINT NOT NULL,
    year SMALLINT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id BIGINT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    age INT,
    email TEXT,
    country TEXT,
    postal_code TEXT,
    pet_type TEXT,
    pet_name TEXT,
    pet_breed TEXT
);

CREATE TABLE IF NOT EXISTS dim_seller (
    seller_id BIGINT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    country TEXT,
    postal_code TEXT
);

CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_key TEXT PRIMARY KEY,
    name TEXT,
    contact TEXT,
    email TEXT,
    phone TEXT,
    address TEXT,
    city TEXT,
    country TEXT
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_key TEXT PRIMARY KEY,
    name TEXT,
    location TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    phone TEXT,
    email TEXT
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id BIGINT PRIMARY KEY,
    name TEXT,
    category TEXT,
    price NUMERIC(12, 2),
    stock_quantity INT,
    pet_category TEXT,
    weight NUMERIC(12, 2),
    color TEXT,
    size TEXT,
    brand TEXT,
    material TEXT,
    description TEXT,
    rating NUMERIC(4, 2),
    reviews INT,
    release_date DATE,
    expiry_date DATE,
    supplier_key TEXT
);

CREATE TABLE IF NOT EXISTS fact_sales (
    sale_event_id TEXT PRIMARY KEY,
    source_file TEXT NOT NULL,
    source_row_number INT NOT NULL,
    original_id BIGINT NOT NULL,
    date_key DATE,
    customer_id BIGINT,
    seller_id BIGINT,
    product_id BIGINT,
    store_key TEXT,
    supplier_key TEXT,
    sale_quantity INT,
    sale_total_price NUMERIC(14, 2)
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON fact_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON fact_sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON fact_sales(product_id);
