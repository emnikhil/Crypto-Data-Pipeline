# Crypto Data Pipeline

## Project Overview

This project implements an end-to-end data pipeline that extracts cryptocurrency data from a public API, processes it using Databricks, and stores the cleaned data in Google BigQuery. The pipeline is orchestrated using Apache Airflow (via Google Cloud Composer) to run daily, ensuring that the most recent data is available for analysis.

![Data Pipeline Diagram](/images/crypto.drawio.svg)

## Technologies Used

- **Google Cloud Platform (GCP)**
  - **Google Cloud Storage (GCS)**: Used for storing raw cryptocurrency data extracted from the API.
  - **Google BigQuery**: Used for storing processed data and performing queries for analysis.
  - **Cloud Composer (Apache Airflow)**: Used for orchestrating and scheduling the entire data pipeline.

- **Databricks**
  - **Scala**: Used for data processing, including data cleaning, transformation, and other ETL tasks.

- **GitHub**: Used for version control to manage project code and documentation.

## Pipeline Steps

1. **Data Extraction**:
   - **Trigger Airflow DAG**: A Directed Acyclic Graph (DAG) is triggered in Airflow to initiate the data extraction process.
   - **API Call**: The DAG makes an API call to retrieve the latest cryptocurrency data for the specified coins (e.g., Bitcoin, Ethereum, etc.).
   - **Store Raw Data**: The raw data is stored in Google Cloud Storage (GCS) as JSON or CSV files for further processing.

2. **Data Processing**:
   - **Databricks Integration**: Databricks is set up to access the raw data stored in GCS.
   - **Data Cleaning**: Using Scala, the raw data is cleaned by handling missing values, removing duplicates, and standardizing formats.
   - **Data Transformation**: Additional transformations are applied to make the data analysis-ready (e.g., aggregating, filtering, or restructuring data).
   - **Write Processed Data**: The cleaned and transformed data is written back to Google Cloud Storage or directly to Google BigQuery for analysis.

3. **Data Storage**:
   - **BigQuery Storage**: The processed data is loaded into Google BigQuery, where it can be queried efficiently.
   - **Data Structure**: Define a suitable schema for the BigQuery tables to facilitate efficient querying and reporting.

4. **Data Visualization**:
   - **Google Data Studio**: Connect Google Data Studio to BigQuery to create visualizations and dashboards that display key metrics and insights from the cryptocurrency data.
   - **Report Creation**: Build interactive reports that can be shared with stakeholders or used for further analysis.

5. **Scheduling and Monitoring**:
   - **Daily Runs**: The Airflow DAG is configured to run daily, ensuring that the pipeline continually ingests the latest cryptocurrency data.
   - **Monitoring**: Set up alerts and logs to monitor the execution of the pipeline and ensure data integrity.

## Getting Started

### Prerequisites

- A Google Cloud Platform account
- A Databricks account
- Git installed on your local machine

### Steps to Set Up

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/emnikhil/Crypto-Data-Pipeline/crypto-data-pipeline.git
   cd crypto-data-pipeline
