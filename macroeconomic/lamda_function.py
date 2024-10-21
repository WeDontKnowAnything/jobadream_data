import requests
import pandas as pd
import json
import boto3
from io import BytesIO
from dotenv import load_dotenv
import os
from constants import API_KEY, S3_BUCKET, S3_KEY_PREFIX

s3 = boto3.client('s3')
def fetch_data(endpoint, year):
    url = f'https://ecos.bok.or.kr/api/{endpoint}/{API_KEY}/json/kr/1/100/{year}'
    response = requests.get(url)
    return response.json()

def process_data(year):
    data = {
        'GDP': fetch_data('KeyStatisticList', year),
        'Money Supply': fetch_data('KeyStatisticList', year),
        'PPI': fetch_data('KeyStatisticList', year),
        'CPI': fetch_data('KeyStatisticList', year),
        'BSI': fetch_data('KeyStatisticList', year),
        'Interest Rate': fetch_data('KeyStatisticList', year),
        'Business Cycle': fetch_data('KeyStatisticList', year)
    }

    dfs = {
        key: pd.DataFrame(value['KeyStatisticList']['row']) for key, value in data.items()
    }

    dfs['GDP'] = dfs['GDP'][dfs['GDP']['CLASS_NAME'] == '소득']
    dfs['Money Supply'] = dfs['Money Supply'][dfs['Money Supply']['CLASS_NAME'] == '통화량']
    dfs['PPI'] = dfs['PPI'][dfs['PPI']['KEYSTAT_NAME'] == '생산자물가지수']
    dfs['CPI'] = dfs['CPI'][dfs['CPI']['KEYSTAT_NAME'] == '소비자물가지수']
    dfs['BSI'] = dfs['BSI'][dfs['BSI']['CLASS_NAME'] == '기업경영지표']
    dfs['Interest Rate'] = dfs['Interest Rate'][dfs['Interest Rate']['CLASS_NAME'] == '시장금리']
    dfs['Business Cycle'] = dfs['Business Cycle'][dfs['Business Cycle']['CLASS_NAME'] == '경기순환지표']

    for key in dfs.keys():
        dfs[key]['YEAR'] = year
        dfs[key]['DATA_SOURCE'] = key

    return pd.concat(list(dfs.values()), ignore_index=True)

def save_to_s3_parquet(df, year):
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, compression='zstd')
    
    s3_key = f"{S3_KEY_PREFIX}data_{year}.parquet"
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=parquet_buffer.getvalue())
    print(f"Uploaded {s3_key} to S3")

def lambda_handler(event, context):
    years = range(2020, 2025)

    for year in years:
        combined_df = process_data(year)

        save_to_s3_parquet(combined_df, year)

    return {
        'statusCode': 200,
        'body': json.dumps('Parquet files uploaded successfully!')
    }

if __name__ == "__main__":
    test_event = {}
    test_context = {}
    print(lambda_handler(test_event, test_context))
