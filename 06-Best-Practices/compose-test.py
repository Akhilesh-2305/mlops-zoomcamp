import os
import pandas as pd



# Localstack endpoint URL
S3_ENDPOINT_URL = 'http://localhost:4566'

def get_input_path(year, month):
    default_input_pattern = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    input_pattern = os.getenv('INPUT_FILE_PATTERN', default_input_pattern)
    return input_pattern.format(year=year, month=month)

#def get_input_path(year, month):
    #return INPUT_FILE_PATTERN.format(year=year, month=month)


#def get_output_path(year, month):
# return OUTPUT_FILE_PATTERN.format(year=year, month=month)
def get_output_path(year, month):
    default_output_pattern = 's3://nyc-duration-prediction-alexey/taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet'
    output_pattern = os.getenv('OUTPUT_FILE_PATTERN', default_output_pattern)
    return output_pattern.format(year=year, month=month)

def read_data(file_path):
    options = {
        'storage_options': {
            'client_kwargs': {
                'endpoint_url': S3_ENDPOINT_URL
            }
        }
    }
    # Check if S3_ENDPOINT_URL is set and use it for reading
    if S3_ENDPOINT_URL:
        options['storage_options']['client_kwargs']['endpoint_url'] = S3_ENDPOINT_URL

    df = pd.read_parquet(file_path, **options)
    return df

#def main():
    #year = 2023
    #month = 3

    #input_file = get_input_path(year, month)
    #output_file = get_output_path(year, month)
def main(year, month):
    input_file = get_input_path(year, month)
    output_file = get_output_path(year, month)

    print(f"Input File Path: {input_file}")
    print(f"Output File Path: {output_file}")

    # Example usage of read_data function
    df = read_data(input_file)
    print(df.head())  # Print first few rows of the DataFrame

if __name__ == "__main__":
    year = 2023
    month = 3
    main(year, month)
