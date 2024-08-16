from airflow import DAG
from airflow.models import Variable  # Corrected from 'variable' to 'Variable'
from datetime import datetime, timedelta
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from bq_pg_config import PG_CONN_ID, PG_SCHEMA, PG_TABLES, BQ_CONN_ID, BQ_PROJECT, BQ_DATASET, BQ_BUCKET, BQ_TABLES, CSV_FILENAMES

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='pg_to_gcs_to_bq',
    default_args=default_args,
    description='airflow DAG to load data into BigQuery',
    schedule_interval=None,
    catchup=False     
)

# Retrieve variables from Airflow Variables
bucket_name = Variable.get("bucket_name")
project_id = Variable.get("project_id")
dataset_id = Variable.get("dataset_id")
postgres_conn_id = Variable.get("pg_conn")
pg_schema = Variable.get("pg_schema")
bq_conn_id = Variable.get("bq_conn")

# Iterate over tables and corresponding filenames
for pg_table, bq_table, csv_filename in zip(PG_TABLES, BQ_TABLES, CSV_FILENAMES):
    
    # Export from PostgreSQL to GCS
    postgres_data_to_gcs = PostgresToGCSOperator(
        task_id=f'export_{pg_table}_to_gcs',
        postgres_conn_id=postgres_conn_id,  # Use the variable 'postgres_conn_id' defined earlier
        sql=f'SELECT * FROM {pg_schema}.{pg_table}',
        bucket=bucket_name,
        filename=f'{pg_table}/{csv_filename}',
        export_format='newline_delimited_json',
        gcp_conn_id=bq_conn_id,
        max_active_tis_per_dag=1,
        gzip=True,
        dag=dag,
    )

    # Load from GCS to BigQuery
    schema_filename = f'schemas/schema_{pg_table}.json'
    gcs_to_bq = GCSToBigQueryOperator(
        task_id=f'load_{bq_table}_to_bq',
        bucket=bucket_name,
        source_objects=[f'{pg_table}/{csv_filename}'],
        destination_project_dataset_table=f'{project_id}:{dataset_id}.{bq_table}',
        schema_object=schema_filename,
        source_format='NEWLINE_DELIMITED_JSON',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id=bq_conn_id,
        dag=dag,
    )

    # Setting task dependencies
    postgres_data_to_gcs >> gcs_to_bq