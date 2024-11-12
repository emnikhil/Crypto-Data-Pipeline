# Crypto Data Pipeline

## Project Overview

This project implements an end-to-end data pipeline that extracts cryptocurrency data from a public API, processes it, and stores the data in Google BigQuery for analysis. The pipeline is orchestrated using Apache Airflow (via Google Cloud Composer) to run daily, ensuring that the most recent data is available for visualization and reporting.

![Data Pipeline Diagram](/images/crypto-drawio.png)

## Technologies Used

- **Google Cloud Platform (GCP)**
  - **Google Cloud Storage (GCS)**: Used for storing raw cryptocurrency data extracted from the API. The data is organized in folders by date.
  - **Google BigQuery**: Used for storing processed data and running procedures for analysis.
  - **Cloud Composer (Apache Airflow)**: Used for orchestrating and scheduling the entire data pipeline, ensuring data is processed and updated daily.

- **GitHub**: Used for version control to manage project code and documentation.

## Pipeline Steps

1. **Data Extraction**:
   - **Trigger Airflow DAG**: A Directed Acyclic Graph (DAG) is triggered in Airflow to start the data extraction process.
   - **API Call**: The DAG makes an API call to retrieve the latest cryptocurrency data for specified coins (e.g., Bitcoin, Ethereum).
   - **Store Raw Data**: The raw data is stored in Google Cloud Storage (GCS) in CSV format, with each file saved in a date-stamped folder (e.g., `crypto_data/YYYY-MM-DD/crypto_data_<timestamp>.csv`).

2. **Data Load to BigQuery**:
   - **BigQuery Load**: After a brief waiting period, a BigQuery load job is triggered to append the CSV data from GCS into an existing BigQuery table. The table structure remains consistent, with only new data appended daily.
   - **Procedure Execution**: After the data is loaded, a BigQuery stored procedure is executed to perform any necessary transformations and calculations, generating output in a final BigQuery table ready for visualization.

3. **Data Visualization**:
   - **Google Data Studio**: Connect Google Data Studio to the final BigQuery table to create visualizations and dashboards displaying cryptocurrency trends and metrics.
   - **Report Creation**: Build interactive reports for stakeholders or further analysis.

4. **Scheduling and Monitoring**:
   - **Daily Runs**: The Airflow DAG is configured to run daily, ensuring that the pipeline continually ingests the latest cryptocurrency data.
   - **Monitoring**: Set up alerts and logs in Airflow to monitor the pipeline’s execution and ensure data accuracy.

## Getting Started

### Prerequisites

- A Google Cloud Platform account
- Git installed on your local machine
