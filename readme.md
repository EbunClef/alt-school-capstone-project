# alt-school-capstone-project

## Project Overview

This project involves developing an end-to-end ETL process using the Brazilian E-Commerce dataset from Kaggle. The project demonstrates knowledge of tools such as PostgreSQL, Docker, Docker Compose, Apache Airflow, dbt, and Google BigQuery.

## Project Architecture

![Architecture Diagram](C:\Users\young.ayodele\Downloads\etl_architecture.png)

*Figure 1: Project Architecture Diagram*

The diagram above illustrates the architecture of the ETL process, including the following components:

1. **Data Source:**
   - Brazilian E-Commerce dataset from Kaggle.

2. **Data Ingestion:**
   - PostgreSQL setup using Docker.
   - Data is ingested into PostgreSQL tables.

3. **Data Orchestration:**
   - Apache Airflow for managing ETL workflows.
   - DAGs to handle data extraction, transformation, and loading.

4. **Data Ingestion:**
   - Transfer data from PostgreSQL to Google BigQuery.

5. **Data Transformation:**
   - dbt models to transform and model data in BigQuery.

6. **Data Analysis:**
   - Queries to answer analytical questions about the dataset.

## Project Steps

### 1. Data Ingestion into PostgreSQL

**Objective:** Ingest data from the Brazilian E-Commerce dataset into a PostgreSQL database.

- **Download the Dataset:** Obtained from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).
- **Setup PostgreSQL Database:** 
  - Docker and Docker Compose are used to set up PostgreSQL.
  - Created a database named `ecommerce`.
- **Create Tables:** Defined table schemas based on CSV files.
- **Ingest Data:** Loaded CSV data into PostgreSQL tables using Python scripts or `init.sql`.

### 2. Setting up Apache Airflow

**Objective:** Orchestrate the ETL process using Airflow.

- **Install Airflow:** Added to the Docker Compose setup.
- **Create Airflow DAG:**
  - Designed a Directed Acyclic Graph (DAG) to manage data extraction from PostgreSQL and loading into BigQuery.
  - Includes tasks for extraction and loading.

### 3. Loading Data from PostgreSQL to BigQuery

**Objective:** Transfer data from PostgreSQL to Google BigQuery.

- **Setup Google BigQuery:**
  - Created a GCP project and enabled BigQuery API.
  - Created a dataset to hold the e-commerce data.
- **Load Data Using Airflow:**
  - Used Airflow operators to transfer data.
  - Included any necessary transformations.

### 4. Transforming and Modeling Data with dbt

**Objective:** Transform and model data using dbt.

- **Setup dbt:**
  - Installed and configured dbt to connect to BigQuery.
- **Create Models:**
  - **Staging Models:**
    - `stg_orders.sql`: Raw orders data.
    - `stg_products.sql`: Raw product data.
  - **Intermediate Models:**
    - `int_sales_by_category.sql`: Aggregated sales data by product category.
    - `int_avg_delivery_time.sql`: Average delivery time calculation.
    - `int_orders_by_state.sql`: Count of orders per state.
  - **Final Models:**
    - `fct_sales_by_category.sql`: Final sales by category.
    - `fct_avg_delivery_time.sql`: Final average delivery time.
    - `fct_orders_by_state.sql`: Final orders by state.

### 5. Answering Analytical Questions

**Objective:** Provide insights based on the dataset.

1. **Which product categories have the highest sales?**
   - Aggregated sales by product category.
2. **What is the average delivery time for orders?**
   - Calculated average delivery time.
3. **Which states have the highest number of orders?**
   - Counted orders per state.

## Project Deliverables

- **PostgreSQL Scripts:** Scripts to create tables and ingest data.
- **Airflow DAG:** The DAG file orchestrating the ETL process.
- **dbt Project:** Models for transforming and modeling data.
- **Analysis:** SQL queries answering the analytical questions.
- **Docker Compose File:** `docker-compose.yml` showing setup of project resources.

## Setup and Usage

### Prerequisites

- Docker
- Docker Compose
- Python (for dbt and Airflow setup)

### Installation

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/your-username/alt-school-capstone-project.git
   cd alt-school-capstone-project
   ```

2. **Setup Docker and Docker Compose:**

   ```bash
   docker-compose up -d
   ```

3. **Setup Airflow:**

   - Initialize Airflow database:

     ```bash
     docker-compose exec airflow airflow db init
     ```

   - Create a new DAG file in `dags/` folder.

4. **Run Airflow DAG:**

   - Access the Airflow web interface at `http://localhost:8080`.
   - Trigger the DAG to start the ETL process.

5. **Setup dbt:**

   - Install dbt and configure it to connect to BigQuery:

     ```bash
     pip install dbt
     dbt init
     ```

   - Run dbt models:

     ```bash
     dbt run
     ```

## Documentation

- **README:** This file.
- **Airflow DAG:** Located in `dags/` folder.
- **dbt Models:** Located in `models/` folder.

## Results

- **SQL Queries/Dashboards:** Find them in `queries/` or `dashboards/` folder.
- **Video Summary:** [Link to video](#)

## License

This project is licensed under the MIT License.

## Acknowledgements

- Kaggle for the dataset.
- Google Cloud Platform for BigQuery.
- Docker and Airflow communities for tools and resources.