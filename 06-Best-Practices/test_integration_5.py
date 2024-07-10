import os
import pandas as pd
import subprocess

# Localstack endpoint URL
S3_ENDPOINT_URL = 'http://localhost:4566'

# Define the S3 bucket and file path
bucket_name = 'nyc-duration'
key = 'in/2023-01.parquet'
input_file = f"s3://{bucket_name}/{key}"

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

def verify_file_creation_and_size():
    # Verify the file is created using AWS CLI
    aws_cli_command = f"aws --endpoint-url={S3_ENDPOINT_URL} s3 ls {input_file}"
    result = subprocess.run(aws_cli_command, shell=True, capture_output=True, text=True)
    
    if result.returncode == 0:
        print("File was successfully created.")
        # Get the file size using AWS CLI --summarize option
        aws_cli_size_command = f"aws --endpoint-url={S3_ENDPOINT_URL} s3 ls {input_file} --summarize"
        size_result = subprocess.run(aws_cli_size_command, shell=True, capture_output=True, text=True)
        
        # Extract and print the total size of the file
        size_output_lines = size_result.stdout.splitlines()
        for line in size_output_lines:
            if line.startswith("Total Size"):
                file_size = int(line.split()[2])
                print(f"File size: {file_size} bytes")
                break
    else:
        print("Error: File creation failed.")

if __name__ == "__main__":
    create_and_save_dataframe()
    verify_file_creation_and_size()
