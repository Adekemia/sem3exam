-- Create database

--CREATE DATABASE ecommerce;

-- Connect to the database
\c ecommerce;


-- Create schema
CREATE SCHEMA IF NOT EXISTS dataset;


-- create and populate tables
create table if not exists dataset.customers
(
    customer_id varchar not null, 
    customer_unique_id uuid not null, 
    customer_zip_code_prefix int not null, 
    customer_city varchar not null, 
    customer_state varchar not null
    
);


COPY DATASET.CUSTOMERS (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
FROM '/data/customers.csv' DELIMITER ',' CSV HEADER;

-- setup geolocation table following the example above
-- DDL statment to create this table dataset.geolocation
create table if not exists dataset.geolocation
(
   geolocation_zip_code_prefix int not null, 
   geolocation_lat numeric not null, 
   geolocation_lng numeric not null, 
   geolocation_city varchar not null, 
   geolocation_state varchar not null

);


-- TODO: provide the command to copy the customers data in the /data folder into DATASET.GEOLOCATION
COPY DATASET.GEOLOCATION (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state)
FROM '/data/geolocation.csv' DELIMITER ',' CSV HEADER;


-- ORDERS_ITEMS table
create table if not exists DATASET.ORDER_ITEMS
(
    order_id varchar not null, 
    order_item_id int not null,
    product_id varchar not null,
    seller_id varchar not null, 
    shipping_limit_date timestamp not null,
    price numeric not null,
    freight_value numeric not null
     
);


-- provide the command to copy orders data into POSTGRES
COPY DATASET.ORDER_ITEMS (order_id, order_item_id,product_id,seller_id, shipping_limit_date,price,freight_value)
FROM '/data/order_items.csv' DELIMITER ',' CSV HEADER;

--ORDERS_PAYMENTS table
create table if not exists DATASET.ORDER_PAYMENTS
(
    order_id varchar not null,
    payment_sequential int not null,
    payment_type varchar not null,
    payment_installments int not null,
    payment_value numeric not null
    
);


-- provide the command to copy DATASET.ORDERS_PAYMENTS data into POSTGRES
COPY DATASET.ORDER_PAYMENTS (order_id,payment_sequential,payment_type,payment_installments,payment_value)
FROM '/data/order_payments.csv' DELIMITER ',' CSV HEADER;

-- setup the ORDER_REVIEWS table following the examle provided
create table if not exists DATASET.ORDER_REVIEWS
(
    review_id varchar not null, 
    order_id varchar not null, 
    review_score int not null, 
    review_comment_title varchar, 
    review_comment_message varchar, 
    review_creation_date timestamp, 
    review_answer_timestamp timestamp
    
);

-- TODO: provide the command to copy DATASET.ORDER_REVIEWS data into POSTGRES
COPY DATASET.ORDER_REVIEWS (review_id, order_id, review_score, review_comment_title, review_comment_message, review_creation_date, review_answer_timestamp)
FROM '/data/order_reviews.csv' DELIMITER ',' CSV HEADER;

-- setup the ORDERS table following the examle provided
create table if not exists DATASET.ORDERS
(
    order_id varchar not null, 
    customer_id varchar not null, 
    order_status varchar, 
    order_purchase_timestamp timestamp, 
    order_approved_at timestamp, 
    order_delivered_carrier_date timestamp, 
    order_delivered_customer_date timestamp, 
    order_estimated_delivery_date timestamp
    
);

-- TODO: provide the command to copy DATASET.ORDERS data into POSTGRES
COPY DATASET.ORDERS (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date)
FROM '/data/orders.csv' DELIMITER ',' CSV HEADER;

-- setup the PRODUCT_CATEGORY_NAME_TRANSLATION table following the examle provided
create table if not exists DATASET.PRODUCT_CATEGORY_NAME_TRANSLATION
(
    product_category_name varchar,
    product_category_name_english varchar

);

-- TODO: provide the command to copy DATASET.PRODUCT_CATEGORY_NAME_TRANSLATION data into POSTGRES
COPY DATASET.PRODUCT_CATEGORY_NAME_TRANSLATION (product_category_name, product_category_name_english)
FROM '/data/product_category_name_translation.csv' DELIMITER ',' CSV HEADER;

-- setup the Products table following the examle provided
create table if not exists DATASET.PRODUCTS
(
    product_id varchar not null, 
    product_category_name varchar,
    product_name_lenght int,
    product_description_lenght int,
    product_photos_qty int, 
    product_weight_g int, 
    product_length_cm int,
    product_height_cm int, 
    product_width_cm int

);

-- TODO: provide the command to copy DATASET.PRODUCTS data into POSTGRES
COPY DATASET.PRODUCTS (product_id, product_category_name, product_name_lenght, product_description_lenght, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm)
FROM '/data/products.csv' DELIMITER ',' CSV HEADER;
 
 
-- sellers table
create table if not exists DATASET.SELLERS
(
    seller_id varchar not null, 
    seller_zip_code_prefix int, 
    seller_city varchar, 
    seller_state varchar

);

-- TODO: provide the command to copy DATASET.PRODUCTS data into POSTGRES
COPY DATASET.SELLERS (seller_id, seller_zip_code_prefix, seller_city, seller_state)
FROM '/data/sellers.csv' DELIMITER ',' CSV HEADER;
 




