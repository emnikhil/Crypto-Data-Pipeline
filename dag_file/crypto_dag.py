from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
import logging

from datetime import datetime, timedelta

import time
import requests
import pandas as pd

from google.cloud import storage

#Function to Fetch data and write on google cloud storage
def fetch_and_upload_crypto_data():
    url = 'https://api.coingecko.com/api/v3/simple/price'

    # Get current date for filename
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    # List of coins you want to fetch
    coins = ['bitcoin', 'ethereum', 'tether', 'binancecoin', 'solana']
    
    # Initialize an empty list to collect all coin data
    all_data = []

    # Loop through each coin and fetch the data separately
    for coin in coins:
        params = {
            'ids': coin,
            'vs_currencies': 'usd,inr,eur',
            'include_1hr_vol': 'true',
            'include_market_cap': 'true',
            'include_24hr_vol': 'true',
            'include_24hr_change': 'true',
            'include_last_updated_at': 'true',
        }
    
        try:
            # Fetch data from the API for each coin individually
            response = requests.get(url, params=params)
            response.raise_for_status()  # Raise an error for bad responses
            
            # Check if the response contains the data for the coin
            data = response.json()

            # Ensure the coin data exists in the response
            if coin in data:
                coin_data = data[coin]

                # Flatten the nested data without appending the coin name to the column
                flattened_data = {
                    'coin': coin,
                    'usd': coin_data.get('usd', None),
                    'usd_market_cap': coin_data.get('usd_market_cap', None),
                    'usd_24h_vol': coin_data.get('usd_24h_vol', None),
                    'usd_24h_change': coin_data.get('usd_24h_change', None),
                    'usd_last_updated': coin_data.get('last_updated_at', None),
                    'inr': coin_data.get('inr', None),
                    'inr_market_cap': coin_data.get('inr_market_cap', None),
                    'inr_24h_vol': coin_data.get('inr_24h_vol', None),
                    'inr_24h_change': coin_data.get('inr_24h_change', None),
                    'inr_last_updated': coin_data.get('last_updated_at', None),
                    'eur': coin_data.get('eur', None),
                    'eur_market_cap': coin_data.get('eur_market_cap', None),
                    'eur_24h_vol': coin_data.get('eur_24h_vol', None),
                    'eur_24h_change': coin_data.get('eur_24h_change', None),
                    'eur_last_updated': coin_data.get('last_updated_at', None),
                    'date': current_date,
                }
                
                all_data.append(flattened_data)  # Append the cleaned data for this coin
                print(f"Data for {coin} appended to DataFrame.")
            else:
                print(f"Data for {coin} not found in the response.")
        
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {coin}: {e}")

        except Exception as e:
            print(f"Error processing {coin}: {e}")
        
        # Wait for 30 seconds before making the next request
        time.sleep(30)

    # Check if any data was collected
    if len(all_data) > 0:
        # Convert all collected data to a DataFrame
        df = pd.DataFrame(all_data)  # Use DataFrame directly from the cleaned list
        
        local_file_path = f'/tmp/crypto_data_{current_date}.csv'
        
        # Save the DataFrame as a csv file
        df.to_csv(local_file_path, index=False)

        # Initialize GCS client and upload the file
        client = storage.Client()
        bucket = client.get_bucket('initial_layer')
        blob = bucket.blob(f'crypto_data/{current_date}/crypto_data_{current_date}.csv')
        blob.upload_from_filename(local_file_path)
        
        print("Data uploaded to GCS successfully.")
        
    else:
        print("No data collected. No file uploaded.")

#Function to load data on BigQuery
def load_data_to_bigquery():

    # Set up logging
    logging.basicConfig(level=logging.INFO)

    client = bigquery.Client()

    # Define BigQuery table ID
    table_id = 'crypto-data-analysis-441505.initial_data.crypto_data_temp'

    # Set up job configuration to append data and create table if needed
    job_config = bigquery.LoadJobConfig(
        source_format = bigquery.SourceFormat.CSV,
        skip_leading_rows = 1,
        autodetect = True,  # Automatically detects the schema
        write_disposition = bigquery.WriteDisposition.WRITE_APPEND,  # Append data to the existing table
        create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED,  # Create the table if it doesn't exist
        schema_update_options = ['ALLOW_FIELD_ADDITION'],  # Allow new fields to be added to the schema
        time_partitioning = bigquery.table.TimePartitioning(
            field = "date",  # Partition Column Name
            type_ = bigquery.TimePartitioningType.DAY  # Partition by day
        )
    )

    # Define the URI for source CSV files
    # Get current date
    current_date = datetime.now().strftime("%Y-%m-%d")
    uri = f'gs://initial_layer/crypto_data/{current_date}/crypto_data_*.csv'

    try:
        # Create a load job to load the data from GCS to BigQuery
        load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)

        # Wait for the job to complete
        load_job.result()

        # Check if the load job was successful
        if load_job.state == 'DONE' and load_job.error_result is None:
            logging.info(f"Data appended to BigQuery table {table_id}.")
        else:
            # Log detailed error message if the load job fails
            logging.error(f"Failed to load data to BigQuery table {table_id}.")
            logging.error(load_job.errors)

    except GoogleAPIError as e:
        # Handle Google API errors, such as network issues or permission problems
        logging.error(f"Google Cloud API error occurred: {e}")

    except Exception as e:
        # Handle other unexpected errors
        logging.error(f"An error occurred: {e}")
    
    # Wait for 10 seconds before making the next request
    time.sleep(10)

# Function to trigger BigQuery procedure
def trigger_bigquery_procedure():

    client = bigquery.Client()

    query = "CALL `crypto-data-analysis-441505.source_codes.sp_crypto_analysis`();"
    
    try:
        # Execute the query (stored procedure)
        query_job = client.query(query)

        # Wait for the job to complete
        query_job.result()

        print("BigQuery procedure executed successfully.")

    except GoogleAPIError as e:
        # Handle API errors
        print(f"Google Cloud API error occurred: {e}")

    except Exception as e:
        # Handle other exceptions
        print(f"An error occurred: {e}")

# Function to log start time
def log_start_time():
    print(f"Start time: {datetime.now()}")

# Function to log end time
def log_end_time():
    print(f"End time: {datetime.now()}")

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
with DAG(
    'crypto_data_pipeline',
    default_args = default_args,
    description = 'A DAG to fetch crypto data, store on GCS, and process in Databricks',
    schedule_interval = '0 3 * * *'  # Run daily at 3:30 AM UTC / 9:00 AM IST
) as dag:

    # Start task to print timestamp
    start = PythonOperator(
        task_id = 'start',
        python_callable = log_start_time,
    )

    # Fetch and upload crypto data task
    fetch_and_upload_data_gcs_task = PythonOperator(
        task_id = 'fetch_and_upload_data_gcs_task',
        python_callable = fetch_and_upload_crypto_data,
    )

    # Load data to BigQuery task
    load_data_bq_task = PythonOperator(
        task_id = 'load_data_bq_task',
        python_callable = load_data_to_bigquery,
    )

    # Trigger BigQuery procedure task
    trigger_procedure_task = PythonOperator(
        task_id = 'trigger_procedure_task',
        python_callable = trigger_bigquery_procedure,
    )

    # End task to print timestamp
    end = PythonOperator(
        task_id = 'end',
        python_callable = log_end_time,
    )

    # Task dependencies
    start >> fetch_and_upload_data_gcs_task >> load_data_bq_task  >> trigger_procedure_task >> end