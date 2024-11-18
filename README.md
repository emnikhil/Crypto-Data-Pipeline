# Crypto Data Pipeline with Airflow and Google Cloud

This project implements an end-to-end data pipeline that extracts cryptocurrency data from a public API, processes it, and stores the data in Google BigQuery for analysis. The pipeline is orchestrated using Apache Airflow (via Google Cloud Composer) to run daily, ensuring that the most recent data is available for visualization and reporting.

![Data Pipeline Diagram](/images/crypto.png)

---

## Data Processing Pipeline Overview

The data pipeline involves multiple stages:

1. **Fetching Crypto Data:**
   - The pipeline fetches real-time cryptocurrency data from external APIs, specifically focusing on 5 selected cryptocurrencies: **'bitcoin', 'ethereum', 'tether', 'binancecoin', 'solana'**.
   - This data is retrieved in CSV format from the [CoinGecko API](https://api.coingecko.com/api/v3/simple/price) and uploaded to a Google Cloud Storage bucket for storage.

2. **Data Transformation:**
   - The fetched data undergoes transformation and is prepared for loading into Google BigQuery.
   - The transformation process includes cleaning, reformatting, and filtering unnecessary data to make it suitable for analysis.

3. **Loading Data to BigQuery:**
   - Once the data is cleaned and transformed, it is loaded into a **BigQuery table**.
   - The data is partitioned by date, and the `date` column in the data is used for partitioning.
   - Data is loaded into a table in BigQuery, where each row represents a cryptocurrency's market data at a given time.

4. **Triggering BigQuery Stored Procedure:**
   - After loading data into BigQuery, a stored procedure is triggered in BigQuery for further data analysis. This stored procedure can be used for performing aggregations, generating insights, or running advanced analytics on the loaded data.

---

## Apache Airflow for Orchestrating the Pipeline

Airflow is used to orchestrate the entire pipeline, allowing you to define, schedule, and monitor each step of the data processing workflow. The pipeline is built with **PythonOperators** in Airflow, and the following steps are executed in sequence:

### Tasks in the Airflow DAG

1. **Start Task (`start`):**
   - This task logs the start timestamp for the pipeline execution.

2. **Fetch Crypto Data Task (`fetch_and_upload_data_gcs_task`):**
   - This task calls the function to fetch cryptocurrency data and uploads it to Google Cloud Storage.

3. **Load Data to BigQuery Task (`load_data_bq_task`):**
   - This task loads the cleaned data from Google Cloud Storage to Google BigQuery.

4. **Trigger BigQuery Procedure Task (`trigger_procedure_task`):**
   - This task triggers the BigQuery procedure after data is loaded, allowing for further analysis.

5. **End Task (`end`):**
   - This task logs the end timestamp, marking the completion of the pipeline.

### Airflow Graph

![Airflow Graph](/images/airflow-graph.png)

![Airflow Task Log](/images/airflow-log.png)

---

## Google Cloud Storage

Google Cloud Storage (GCS) is used as a staging area for storing the fetched cryptocurrency data before it is loaded into BigQuery. GCS allows for easy storage and access of large datasets, making it an ideal choice for this pipeline.

The pipeline fetches cryptocurrency data in real-time and stores it in a GCS bucket. From there, it is processed and loaded into BigQuery.

![Google Cloud Storage](/images/gcs.png)

---

## BigQuery Data Sample

Here’s a snapshot of the data loaded into Google BigQuery after running the pipeline:

![BigQuery Data Sample](/images/bigquery-data-sample.png)

---

## Database Schema

![Database Diagram](/images/database-diagram.png)

The database consists of the following tables:

- **`crypto_data_temp`**: Stores raw cryptocurrency data for each coin in USD, INR, and EUR.
- **`final_data`**: Stores processed data with details for each cryptocurrency in different currencies.
- **`market_metrics`**: Stores total market cap data for each currency.
- **`volume_metrics`**: Stores 24-hour trading volume for each currency.

These tables are linked by the cryptocurrency `coin` and partitioned by `date` to enable efficient querying and data analysis.

---

## Data Visualization with Looker

Looker is used to visualize and analyze the processed cryptocurrency data. With its interactive dashboards and powerful query-building capabilities, Looker enables detailed insights into the performance and trends of selected cryptocurrencies.

### Key Visualizations

1. **Overall Market Trends:**
   - A dashboard showcasing real-time market cap, 24-hour volume, and price changes for the 5 cryptocurrencies.

2. **Performance Comparison:**
   - A comparative analysis of price changes and market trends for **'bitcoin', 'ethereum', 'tether', 'binancecoin', and 'solana'**.

![Looker Visualization 1](/images/looker1.png)
![Looker Visualization 2](/images/looker2.png)

With these visualizations, users can easily track cryptocurrency performance over time and make informed decisions based on up-to-date market data.

---

## Setup and Configuration

### Set up Google Cloud

Before starting, ensure you have set up the following on your Google Cloud account:

- A **GCS bucket** for storing cryptocurrency data.
- A **BigQuery dataset** and table for loading and analyzing data.
- Ensure the necessary IAM roles and permissions are granted for interacting with GCS and BigQuery.