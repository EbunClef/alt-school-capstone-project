
-- Create schema
CREATE SCHEMA IF NOT EXISTS ALT_SCHOOL;


-- create and populate tables
create table if not exists ALT_SCHOOL.OLIST_CUSTOMERS
(
    customer_id varchar,
    customer_unique_id varchar,
    customer_zip_code_prefix varchar,
    customer_city varchar,
    customer_state varchar
);

COPY ALT_SCHOOL.OLIST_CUSTOMERS (customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,customer_state)
FROM '/data/olist_customers_dataset.csv' DELIMITER ',' CSV HEADER;


create table if not exists ALT_SCHOOL.OLIST_GEOLOCATION
(
    geolocation_zip_code_prefix varchar,
    geolocation_lat float, 
    geolocation_lng float,
    geolocation_city varchar,
    geolocation_state varchar
);

COPY ALT_SCHOOL.OLIST_GEOLOCATION (geolocation_zip_code_prefix,geolocation_lat,geolocation_lng,geolocation_city,geolocation_state)
FROM '/data/olist_geolocation_dataset.csv' DELIMITER ',' CSV HEADER;


create table if not exists ALT_SCHOOL.OLIST_ORDER_ITEMS
(
    order_id varchar,
    order_item_id varchar,
    product_id varchar,
    seller_id varchar,
    shipping_limit_date timestamp,
    price float,
    freight_value float
);

COPY ALT_SCHOOL.OLIST_ORDER_ITEMS (order_id,order_item_id,product_id,seller_id,shipping_limit_date,price,freight_value)
FROM '/data/olist_order_items_dataset.csv' DELIMITER ',' CSV HEADER;


create table if not exists ALT_SCHOOL.OLIST_ORDER_PAYMENTS
(
    order_id varchar,
    payment_sequential bigint,
    payment_type varchar,
    payment_installments bigint,
    payment_value float
);

COPY ALT_SCHOOL.OLIST_ORDER_PAYMENTS (order_id,payment_sequential,payment_type,payment_installments,payment_value)
FROM '/data/olist_order_payments_dataset.csv' DELIMITER ',' CSV HEADER;


create table if not exists ALT_SCHOOL.OLIST_ORDER_REVIEWS
(
    review_id varchar,
    order_id varchar,
    review_score bigint,
    review_comment_title varchar,
    review_comment_message varchar,
    review_creation_date timestamp,
    review_answer_timestamp timestamp  
);

COPY ALT_SCHOOL.OLIST_ORDER_REVIEWS (review_id,order_id,review_score,review_comment_title,review_comment_message,review_creation_date,review_answer_timestamp)
FROM '/data/olist_order_reviews_dataset.csv' DELIMITER ',' CSV HEADER;


create table if not exists ALT_SCHOOL.OLIST_ORDERS
(
    order_id varchar,
    customer_id varchar,
    order_status varchar,
    order_purchase_timestamp timestamp,
    order_approved_at timestamp,
    order_delivered_carrier_date timestamp,
    order_delivered_customer_date timestamp,
    order_estimated_delivery_date timestamp
);

COPY ALT_SCHOOL.OLIST_ORDERS (order_id,customer_id,order_status,order_purchase_timestamp,order_approved_at,order_delivered_carrier_date,order_delivered_customer_date,order_estimated_delivery_date)
FROM '/data/olist_orders_dataset.csv' DELIMITER ',' CSV HEADER;


create table if not exists ALT_SCHOOL.OLIST_PRODUCTS
(
    product_id varchar,
    product_category_name varchar,
    product_name_lenght bigint,
    product_description_lenght bigint,
    product_photos_qty bigint,
    product_weight_g bigint,
    product_length_cm bigint,
    product_height_cm bigint,
    product_width_cm bigint
);

COPY ALT_SCHOOL.OLIST_PRODUCTS (product_id,product_category_name,product_name_lenght,product_description_lenght,product_photos_qty,product_weight_g,product_length_cm,product_height_cm,product_width_cm)
FROM '/data/olist_products_dataset.csv' DELIMITER ',' CSV HEADER;


create table if not exists ALT_SCHOOL.OLIST_SELLERS
(
    seller_id varchar,
    seller_zip_code_prefix bigint,
    seller_city varchar,
    seller_state varchar
);

COPY ALT_SCHOOL.OLIST_SELLERS (seller_id,seller_zip_code_prefix,seller_city,seller_state)
FROM '/data/olist_sellers_dataset.csv' DELIMITER ',' CSV HEADER;


create table if not exists ALT_SCHOOL.PRODUCT_CATEGORY_NAME_TRANSLATION
(
    product_category_name varchar,
    product_category_name_english varchar
);

COPY ALT_SCHOOL.PRODUCT_CATEGORY_NAME_TRANSLATION (product_category_name,product_category_name_english)
FROM '/data/product_category_name_translation.csv' DELIMITER ',' CSV HEADER;