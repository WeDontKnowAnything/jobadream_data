import pandas as pd

# CSV 파일 경로 설정
file_path = "/data/listed_corps.csv"

# CSV 파일 로드 및 '회사명' 칼럼만 선택
df = pd.read_csv(file_path)
df_filtered = df[["회사명"]]

# 새로운 CSV 파일로 저장
output_path = "/data/filtered_listed_corps.csv"
df_filtered.to_csv(output_path, index=False)
