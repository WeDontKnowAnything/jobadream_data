import os

from constants import Path, Report
import dart_fss
import pandas as pd
from dotenv import load_dotenv
import pyarrow.parquet as pq
import pyarrow as pa

load_dotenv()


def set_api_key(key_index):
    if key_index == 1:
        API_KEY = os.environ.get("DART_API_KEY1")
    elif key_index == 2:
        API_KEY = os.environ.get("DART_API_KEY2")
    else:
        raise ValueError("Invalid key index")
    dart_fss.set_api_key(api_key=API_KEY)


# corp = 회사
# listed = 상장
# delisted = 비상장
def get_listed_corps_df() -> pd.DataFrame:
    return pd.read_csv(Path.LISTED_PATH.value)


def get_report(corp_df, corp_name, bsns_year, report_code):
    # (1) corp_code : 기업코드
    corp_code = corp_df[corp_df["corp_name"] == corp_name].iloc[
        0, 0
    ]  # corp_df에서 corp_name에 해당하는 corp_code를 찾아 corp_code에 저장

    report_name = Report.REPORT_LIST.value.get(report_code)

    # 만약 연결제무제표가 없다면 재무제표로 다시 시도
    try:
        data = dart_fss.api.finance.fnltt_singl_acnt_all(
            corp_code, bsns_year, report_code, Report.FS_DIV.value[0], api_key=None
        )["list"]
    except:
        try:
            data = dart_fss.api.finance.fnltt_singl_acnt_all(
                corp_code,
                bsns_year,
                report_code,
                Report.FS_DIV.value[1],
                api_key=None,
            )["list"]
        except:
            pass
        else:
            financial_report_df = pd.DataFrame(data)
            financial_report_df.to_csv(
                f"../data/financial_reports/{corp_name} {bsns_year}년 {report_name} 재무보고서.csv",
                index=False,
                encoding="utf-8-sig",
            )
    else:
        financial_report_df = pd.DataFrame(data)
        financial_report_df.to_csv(
            f"../data/financial_reports/{corp_name}{bsns_year}{report_name}.csv",
            index=False,
            encoding="utf-8-sig",
        )


def get_reports(public_companys_df):
    corp_names = list(public_companys_df["corp_name"])
    report_codes = list(Report.REPORT_LIST.value.keys())
    call_count = 0
    start_index = corp_names.index(Report.START_CORP_NAME.value)
    for corp_name in corp_names[start_index:]:
        for bsns_year in Report.BSNS_YEARS.value:
            for report_code in report_codes:
                get_report(public_companys_df, corp_name, bsns_year, report_code)
                call_count += 1
        print(f"{corp_name}사의 재무제표 수집 완료, api호출 수 : {call_count}")


def main():
    for key_index in range(1, 3):
        set_api_key(key_index)
        corps_df = get_listed_corps_df()
        get_reports(corps_df)


if __name__ == "__main__":
    main()
