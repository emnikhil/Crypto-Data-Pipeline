import requests
import pandas as pd
from google.cloud import storage
from datetime import datetime

def fetch_and_upload_crypto_data():
    url = 'https://api.coingecko.com/api/v3/simple/price'
    params = {
        'ids': 'bitcoin,ethereum,cardano,solana,ripple',
        'vs_currencies': 'usd,inr,eur',
        'include_1hr_vol': 'true',
        'include_market_cap': 'true',
        'include_24hr_vol': 'true',
        'include_24hr_change': 'true',
        'include_last_updated_at': 'true',
    }
    
    try:
        # Fetch data from the API
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise an error for bad responses
        data = response.json()

        # Convert to DataFrame and save as Parquet locally
        df = pd.json_normalize(data, sep=',')
        
        # Get current timestamp for filename
        current_time = datetime.now().strftime("%Y%m%d")
        local_file_path = f'/tmp/crypto_data_{current_time}.parquet'
        df.to_parquet(local_file_path, index=False)

        # Initialize GCS client and upload the file
        client = storage.Client()
        bucket = client.get_bucket('your-gcs-bucket-name')  # Replace with your GCS bucket name
        blob = bucket.blob(f'crypto_data/crypto_data_{current_time}.parquet')
        blob.upload_from_filename(local_file_path)
        
        print("Data uploaded to GCS successfully.")
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
    except Exception as e:
        print(f"Error: {e}")

