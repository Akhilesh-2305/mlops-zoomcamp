import pandas as pd

def read_data(year, month, categorical):
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02}.parquet'
    df = pd.read_parquet(url, engine='pyarrow')
    if categorical:
        df['passenger_count'] = df['passenger_count'].astype('category')
    return df

def main(year, month, categorical):
    # Perform refactored tasks here
    df = read_data(year, month, categorical)

    # Example processing
    # Replace with your actual processing logic
    print(f"Data loaded for {year}-{month:02}")
    print(df.head())

    # Example output to local filesystem
    output_file = f'taxi_type=yellow_year={year:04d}_month={month:02d}.parquet'
    df.to_parquet(output_file, engine='pyarrow')

if __name__ == "__main__":
    year = 2023
    month = 3
    categorical = True  # Adjust based on your needs

    main(year, month, categorical)
