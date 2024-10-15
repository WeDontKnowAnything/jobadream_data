import requests
import pandas as pd

api_key = "90OHC6NJN8V2M526I7VJ"

def fetch_data(endpoint, year):
    url = f'https://ecos.bok.or.kr/api/{endpoint}/{api_key}/json/kr/1/100/{year}'
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
        print(key)
        dfs[key]['YEAR'] = year
        dfs[key]['DATA_SOURCE'] = key


    return pd.concat(list(dfs.values()), ignore_index=True)

def main():

    years = range(2020, 2025)
    dataframes = []
    
    for year in years:
        combined_df = process_data(year)
        dataframes.append(combined_df)

    final_combined_df = pd.concat(dataframes, ignore_index=True)

    print(final_combined_df)

    final_combined_df.to_csv('combined_data_2020_2024.csv', index=False)
    final_combined_df.to_pickle('combined_data_2020_2024.pkl')

if __name__ == "__main__":
    main()
