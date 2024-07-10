import os
import pandas as pd
import subprocess

# Localstack endpoint URL
S3_ENDPOINT_URL = 'http://localhost:4566'

# Define the S3 bucket and file paths
bucket_name = 'nyc-duration'
input_key = 'in/2023-01.parquet'
input_file = f"s3://{bucket_name}/{input_key}"
predictions_key = 'out/2023-01-predictions.parquet'
predictions_file = f"s3://{bucket_name}/{predictions_key}"

# Set up storage options for Localstack
options = {
    'client_kwargs': {
        'endpoint_url': S3_ENDPOINT_URL
    }
}

def create_and_save_dataframe():
    # Create a sample DataFrame (assuming you have it from Q3)
    df_input = pd.DataFrame({
        'A': [1, 2, 3],
        'B': [4, 5, 6]
    })
    
    # Save DataFrame to Parquet format in S3
    df_input.to_parquet(
        input_file,
        engine='pyarrow',
        compression=None,
        index=False,
        storage_options=options
    )

def run_batch_script():
    # Run batch.py script for January 2023
    batch_command = f"python batch.py --input {input_file} --output {predictions_file}"
    subprocess.run(batch_command, shell=True)

def read_data(file_path):
    try:
        df = pd.read_parquet(file_path, engine='pyarrow', storage_options=options)
        return df
    except Exception as e:
        print(f"Error reading data from {file_path}: {e}")
        return None

def calculate_sum_of_predictions():
    try:
        df = read_data(predictions_file)
        if df is not None:
            sum_predicted_duration = df['predicted_duration'].sum()
            print(f"Sum of predicted durations: {sum_predicted_duration}")
    except Exception as e:
        print(f"Error calculating sum of predicted durations: {e}")

if __name__ == "__main__":
    # Step 1: Create and save the dataframe to S3
    create_and_save_dataframe()
    
    # Step 2: Run the batch processing script
    run_batch_script()
    
    # Step 3: Calculate the sum of predicted durations
    calculate_sum_of_predictions()
