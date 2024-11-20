import requests
import pandas as pd
import os
from dotenv import load_dotenv
from constants import API_KEY

# 데이터를 API로 가져오는 함수
def fetch_data(endpoint, year):
    url = f'https://ecos.bok.or.kr/api/{endpoint}/{API_KEY}/json/kr/1/100/{year}'
    response = requests.get(url)
    return response.json()

# 데이터를 처리하는 함수
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

    # DataFrame으로 변환
    print(data)
    dfs = {
        key: pd.DataFrame(value['KeyStatisticList']['row']) for key, value in data.items()
    }

    # 필요한 데이터 필터링
    dfs['GDP'] = dfs['GDP'][dfs['GDP']['CLASS_NAME'] == '소득']
    dfs['Money Supply'] = dfs['Money Supply'][dfs['Money Supply']['CLASS_NAME'] == '통화량']
    dfs['PPI'] = dfs['PPI'][dfs['PPI']['KEYSTAT_NAME'] == '생산자물가지수']
    dfs['CPI'] = dfs['CPI'][dfs['CPI']['KEYSTAT_NAME'] == '소비자물가지수']
    dfs['BSI'] = dfs['BSI'][dfs['BSI']['CLASS_NAME'] == '기업경영지표']
    dfs['Interest Rate'] = dfs['Interest Rate'][dfs['Interest Rate']['CLASS_NAME'] == '시장금리']
    dfs['Business Cycle'] = dfs['Business Cycle'][dfs['Business Cycle']['CLASS_NAME'] == '경기순환지표']

    # 'KEYSTAT_NAME'과 'UNIT_NAME' 컬럼을 제거
    for key in dfs.keys():
        dfs[key] = dfs[key].drop(columns=['KEYSTAT_NAME', 'UNIT_NAME'], errors='ignore')
        dfs[key]['YEAR'] = year
        dfs[key]['DATA_SOURCE'] = key
    print(dfs)
    return dfs

# 여러 연도 데이터를 합치는 함수
def merge_data(years):
    merged_dfs = {}
    
    for year in years:
        year_data = process_data(year)
        for key, df in year_data.items():
            if key not in merged_dfs:
                merged_dfs[key] = df
            else:
                merged_dfs[key] = pd.concat([merged_dfs[key], df], ignore_index=True)
    
    return merged_dfs

# CSV 파일로 저장하는 함수 (모든 컬럼을 하나의 CSV로)
# def save_to_csv(merged_dfs):
#     for key, df in merged_dfs.items():
#         file_name = f"macroeconomic_{key}.csv"
#         df.to_csv(file_name, index=False, encoding='utf-8-sig')
#         print(f"{file_name} saved successfully.")

if __name__ == "__main__":
    years = range(2020, 2025)
    merged_dfs = merge_data(years)
    save_to_csv(merged_dfs)
