import csv
import itertools
import os

import boto3
from constants import S3DirPathValues
from dotenv import load_dotenv

load_dotenv()


class __S3DirectoryCreator:
    def __init__(
        self,
        bucket_name,
        corporations_csv_path,
        aws_access_key_id,
        aws_secret_access_key,
        region_name,
    ):
        # AWS 자격 증명 및 region 설정
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )
        self.bucket_name = bucket_name
        self.corporations_csv_path = corporations_csv_path
        self.corporations = self.load_corporations()

    def load_corporations(self):
        corporations = []
        with open(self.corporations_csv_path, mode="r", encoding="utf-8-sig") as file:
            csv_reader = csv.reader(file)
            next(csv_reader)  # 헤더 skip
            for row in csv_reader:
                corporations.append(row[2])  # 기업 이름 -> 2번 index
        return corporations

    def create_directory(self, corporation, year, quarter, statement_type):
        key = f"category=financial/corporation_name={corporation}/year={year}/quarter={quarter}/statement_type={statement_type}/"
        self.s3.put_object(Bucket=self.bucket_name, Key=key)
        print(f"Created directory: {key}")

    def create_all_directories(self, years, quarters, statement_types):
        combinations = itertools.product(
            self.corporations, years, quarters, statement_types
        )
        for corporation, year, quarter, statement_type in combinations:
            self.create_directory(corporation, year, quarter, statement_type)


# def get_corps_df() -> pd.DataFrame:
#     all_corps = dart_fss.api.filings.get_corp_code()
#     all_corps_df = pd.DataFrame(all_corps)
#     corps_df = all_corps_df[all_corps_df["stock_code"].notnull()].reset_index(drop=True)
#     return corps_df


# # 상폐 기업 제거 함수
# def remove_delisted_corps(corp_df) -> pd.DataFrame:
#     # CSV 파일 읽기

#     compare_df = pd.read_csv(Path.CSV2COMPARE_FILE.value)

#     # 데이터 타입 변환
#     corp_df["stock_code"] = corp_df["stock_code"].astype(int)
#     compare_df["종목코드"] = compare_df["종목코드"].astype(int)

#     # 'stock_code'와 '종목코드'가 일치하는 행만 추출
#     merged_df = pd.merge(corp_df, compare_df, left_on="stock_code", right_on="종목코드")
#     result_df = merged_df[["corp_code", "corp_name", "stock_code"]].sort_values(
#         by="corp_name"
#     )

#     return result_df


def create_s3_directories():
    AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
    AWS_CORPORATIONS_CSV_PATH = os.getenv("AWS_CORPORATIONS_CSV_PATH")

    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    REGION_NAME = os.getenv("REGION_NAME")

    YEARS = S3DirPathValues.YEARS.value
    QUARTERS = S3DirPathValues.QUARTERS.value
    STATEMENT_TYPES = S3DirPathValues.STATEMENT_TYPES.value

    s3_creator = __S3DirectoryCreator(
        AWS_BUCKET_NAME,
        AWS_CORPORATIONS_CSV_PATH,
        AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY,
        REGION_NAME,
    )

    s3_creator.__create_all_directories(YEARS, QUARTERS, STATEMENT_TYPES)
