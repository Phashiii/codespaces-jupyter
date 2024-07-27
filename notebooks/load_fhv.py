import pandas as pd
from google.cloud import bigquery

taxi_dtypes = {
                'dispatching_base_num': str,
                'PUlocationID': pd.Int64Dtype(),
                'DOlocationID': pd.Int64Dtype(),
                'SR_Flag': pd.Int64Dtype(),
                'Affiliated_base_number': str
            }
parse_dates = ['pickup_datetime','dropOff_datetime']

year = 1
month = 1
while month < 13:
    if year == 1:
        if month < 10:
            url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-0{month}.csv.gz'
        else:
            url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-{month}.csv.gz'
    elif year == 2:
        if month < 10:
            url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2020-0{month}.csv.gz'
        else:
            url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2020-{month}.csv.gz'
    else:
        url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2021-0{month}.csv.gz'
    
    df = pd.read_csv(url, sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates)
    
    client = bigquery.Client()  

    table_id = 'promptmebebe.ny_taxi.fhv_trips_2019-2020' 

    if month == 1 and year == 1:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    else:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete.   

    table = client.get_table(table_id)  # Make an API request.  

    job = client.load_table_from_dataframe(df, table_id)
    print(job.result())
    

    if month == 12:
        month = 1
        year += 1
        print(f'the year is {year}')
    if year == 3 and month == 7:
        break
    
    month+=1

