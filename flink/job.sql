SET 'execution.runtime-mode' = 'streaming';
SET 'table.local-time-zone' = 'UTC';
SET 'pipeline.name' = 'ABDLab3 Kafka to PostgreSQL star schema';

CREATE TABLE kafka_sales (
    id BIGINT,
    source_file STRING,
    source_row_number INT,
    customer_first_name STRING,
    customer_last_name STRING,
    customer_age INT,
    customer_email STRING,
    customer_country STRING,
    customer_postal_code STRING,
    customer_pet_type STRING,
    customer_pet_name STRING,
    customer_pet_breed STRING,
    seller_first_name STRING,
    seller_last_name STRING,
    seller_email STRING,
    seller_country STRING,
    seller_postal_code STRING,
    product_name STRING,
    product_category STRING,
    product_price DECIMAL(12, 2),
    product_quantity INT,
    sale_date STRING,
    sale_customer_id BIGINT,
    sale_seller_id BIGINT,
    sale_product_id BIGINT,
    sale_quantity INT,
    sale_total_price DECIMAL(14, 2),
    store_name STRING,
    store_location STRING,
    store_city STRING,
    store_state STRING,
    store_country STRING,
    store_phone STRING,
    store_email STRING,
    pet_category STRING,
    product_weight DECIMAL(12, 2),
    product_color STRING,
    product_size STRING,
    product_brand STRING,
    product_material STRING,
    product_description STRING,
    product_rating DECIMAL(4, 2),
    product_reviews INT,
    product_release_date STRING,
    product_expiry_date STRING,
    supplier_name STRING,
    supplier_contact STRING,
    supplier_email STRING,
    supplier_phone STRING,
    supplier_address STRING,
    supplier_city STRING,
    supplier_country STRING,
    event_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'sales',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'abdlab3-flink',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true',
    'json.fail-on-missing-field' = 'false'
);

CREATE VIEW normalized_sales AS
SELECT
    id,
    source_file,
    source_row_number,
    sale_customer_id,
    sale_seller_id,
    sale_product_id,
    MD5(CONCAT(COALESCE(store_name, ''), '|', COALESCE(store_location, ''), '|', COALESCE(store_city, ''), '|', COALESCE(store_country, ''))) AS store_key,
    MD5(CONCAT(COALESCE(supplier_name, ''), '|', COALESCE(supplier_email, ''), '|', COALESCE(supplier_phone, ''), '|', COALESCE(supplier_address, ''))) AS supplier_key,
    TO_DATE(sale_date, 'M/d/yyyy') AS sale_date_key,
    customer_first_name,
    customer_last_name,
    customer_age,
    customer_email,
    customer_country,
    customer_postal_code,
    customer_pet_type,
    customer_pet_name,
    customer_pet_breed,
    seller_first_name,
    seller_last_name,
    seller_email,
    seller_country,
    seller_postal_code,
    product_name,
    product_category,
    product_price,
    product_quantity,
    sale_quantity,
    sale_total_price,
    store_name,
    store_location,
    store_city,
    store_state,
    store_country,
    store_phone,
    store_email,
    pet_category,
    product_weight,
    product_color,
    product_size,
    product_brand,
    product_material,
    product_description,
    product_rating,
    product_reviews,
    TO_DATE(product_release_date, 'M/d/yyyy') AS product_release_date_key,
    TO_DATE(product_expiry_date, 'M/d/yyyy') AS product_expiry_date_key,
    supplier_name,
    supplier_contact,
    supplier_email,
    supplier_phone,
    supplier_address,
    supplier_city,
    supplier_country
FROM kafka_sales;

CREATE TABLE dim_date (
    date_key DATE,
    day SMALLINT,
    month SMALLINT,
    quarter SMALLINT,
    year SMALLINT,
    PRIMARY KEY (date_key) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/abd_lab3',
    'table-name' = 'dim_date',
    'username' = 'postgres',
    'password' = 'secret',
    'driver' = 'org.postgresql.Driver'
);

CREATE TABLE dim_customer (
    customer_id BIGINT,
    first_name STRING,
    last_name STRING,
    age INT,
    email STRING,
    country STRING,
    postal_code STRING,
    pet_type STRING,
    pet_name STRING,
    pet_breed STRING,
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/abd_lab3',
    'table-name' = 'dim_customer',
    'username' = 'postgres',
    'password' = 'secret',
    'driver' = 'org.postgresql.Driver'
);

CREATE TABLE dim_seller (
    seller_id BIGINT,
    first_name STRING,
    last_name STRING,
    email STRING,
    country STRING,
    postal_code STRING,
    PRIMARY KEY (seller_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/abd_lab3',
    'table-name' = 'dim_seller',
    'username' = 'postgres',
    'password' = 'secret',
    'driver' = 'org.postgresql.Driver'
);

CREATE TABLE dim_supplier (
    supplier_key STRING,
    name STRING,
    contact STRING,
    email STRING,
    phone STRING,
    address STRING,
    city STRING,
    country STRING,
    PRIMARY KEY (supplier_key) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/abd_lab3',
    'table-name' = 'dim_supplier',
    'username' = 'postgres',
    'password' = 'secret',
    'driver' = 'org.postgresql.Driver'
);

CREATE TABLE dim_store (
    store_key STRING,
    name STRING,
    location STRING,
    city STRING,
    state STRING,
    country STRING,
    phone STRING,
    email STRING,
    PRIMARY KEY (store_key) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/abd_lab3',
    'table-name' = 'dim_store',
    'username' = 'postgres',
    'password' = 'secret',
    'driver' = 'org.postgresql.Driver'
);

CREATE TABLE dim_product (
    product_id BIGINT,
    name STRING,
    category STRING,
    price DECIMAL(12, 2),
    stock_quantity INT,
    pet_category STRING,
    weight DECIMAL(12, 2),
    color STRING,
    size STRING,
    brand STRING,
    material STRING,
    description STRING,
    rating DECIMAL(4, 2),
    reviews INT,
    release_date DATE,
    expiry_date DATE,
    supplier_key STRING,
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/abd_lab3',
    'table-name' = 'dim_product',
    'username' = 'postgres',
    'password' = 'secret',
    'driver' = 'org.postgresql.Driver'
);

CREATE TABLE fact_sales (
    sale_event_id STRING,
    source_file STRING,
    source_row_number INT,
    original_id BIGINT,
    date_key DATE,
    customer_id BIGINT,
    seller_id BIGINT,
    product_id BIGINT,
    store_key STRING,
    supplier_key STRING,
    sale_quantity INT,
    sale_total_price DECIMAL(14, 2),
    PRIMARY KEY (sale_event_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/abd_lab3',
    'table-name' = 'fact_sales',
    'username' = 'postgres',
    'password' = 'secret',
    'driver' = 'org.postgresql.Driver'
);

EXECUTE STATEMENT SET
BEGIN
INSERT INTO dim_date
SELECT DISTINCT
    sale_date_key,
    CAST(EXTRACT(DAY FROM sale_date_key) AS SMALLINT),
    CAST(EXTRACT(MONTH FROM sale_date_key) AS SMALLINT),
    CAST(EXTRACT(QUARTER FROM sale_date_key) AS SMALLINT),
    CAST(EXTRACT(YEAR FROM sale_date_key) AS SMALLINT)
FROM normalized_sales
WHERE sale_date_key IS NOT NULL;

INSERT INTO dim_customer
SELECT DISTINCT
    sale_customer_id,
    customer_first_name,
    customer_last_name,
    customer_age,
    customer_email,
    customer_country,
    customer_postal_code,
    customer_pet_type,
    customer_pet_name,
    customer_pet_breed
FROM normalized_sales
WHERE sale_customer_id IS NOT NULL;

INSERT INTO dim_seller
SELECT DISTINCT
    sale_seller_id,
    seller_first_name,
    seller_last_name,
    seller_email,
    seller_country,
    seller_postal_code
FROM normalized_sales
WHERE sale_seller_id IS NOT NULL;

INSERT INTO dim_supplier
SELECT DISTINCT
    supplier_key,
    supplier_name,
    supplier_contact,
    supplier_email,
    supplier_phone,
    supplier_address,
    supplier_city,
    supplier_country
FROM normalized_sales
WHERE supplier_key IS NOT NULL;

INSERT INTO dim_store
SELECT DISTINCT
    store_key,
    store_name,
    store_location,
    store_city,
    store_state,
    store_country,
    store_phone,
    store_email
FROM normalized_sales
WHERE store_key IS NOT NULL;

INSERT INTO dim_product
SELECT DISTINCT
    sale_product_id,
    product_name,
    product_category,
    product_price,
    product_quantity,
    pet_category,
    product_weight,
    product_color,
    product_size,
    product_brand,
    product_material,
    product_description,
    product_rating,
    product_reviews,
    product_release_date_key,
    product_expiry_date_key,
    supplier_key
FROM normalized_sales
WHERE sale_product_id IS NOT NULL;

INSERT INTO fact_sales
SELECT
    CONCAT(source_file, ':', CAST(source_row_number AS STRING)),
    source_file,
    source_row_number,
    id,
    sale_date_key,
    sale_customer_id,
    sale_seller_id,
    sale_product_id,
    store_key,
    supplier_key,
    sale_quantity,
    sale_total_price
FROM normalized_sales;
END;
