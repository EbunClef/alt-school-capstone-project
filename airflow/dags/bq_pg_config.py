import os
from datetime import datetime

# BigQuery Configuration Variables
BQ_CONN_ID = "gcp_conn"
BQ_PROJECT = os.getenv('BQ_PROJECT')
BQ_DATASET = os.getenv('BQ_DATASET')
BQ_BUCKET = os.getenv('BQ_BUCKET')

# BigQuery Tables
BQ_TABLES = [
    "olist_customers",
    "olist_geolocation",
    "olist_order_items",
    "olist_order_payments",
    "olist_order_reviews",
    "olist_orders",
    "olist_products",
    "olist_sellers",
    "product_category_name_translation"
]

# Corresponding CSV Filenames
CSV_FILENAMES = [
    'customers_data_' + datetime.now().strftime('%Y-%m-%d') + '.csv',
    'geolocation_data_' + datetime.now().strftime('%Y-%m-%d') + '.csv',
    'order_items_data_' + datetime.now().strftime('%Y-%m-%d') + '.csv',
    'order_payments_data_' + datetime.now().strftime('%Y-%m-%d') + '.csv',
    'order_reviews_data_' + datetime.now().strftime('%Y-%m-%d') + '.csv',
    'orders_data_' + datetime.now().strftime('%Y-%m-%d') + '.csv',
    'products_data_' + datetime.now().strftime('%Y-%m-%d') + '.csv',
    'sellers_data_' + datetime.now().strftime('%Y-%m-%d') + '.csv',
    'product_category_name_translation_data_' + datetime.now().strftime('%Y-%m-%d') + '.csv'
]

# PostgreSQL Configuration Variables
PG_CONN_ID = "pg_conn"
PG_SCHEMA = "ALT_SCHOOL"
PG_TABLES = [
    "olist_customers",
    "olist_geolocation",
    "olist_order_items",
    "olist_order_payments",
    "olist_order_reviews",
    "olist_orders",
    "olist_products",
    "olist_sellers",
    "product_category_name_translation"
]

