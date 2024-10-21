import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.environ.get('KOREA_BANK_API_KEY')
ENDPOINT = "KeyStatisticList"

S3_BUCKET = "jobadream-raw-data-bucket"
S3_KEY_PREFIX = "category=macroeconomic/"

YEARS = range(2020, 2025)

