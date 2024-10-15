import ast
import os

from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# .env 파일에서 문자열 형태로 불러오기
api_key = os.getenv("DART_API_KEY")

# 문자열을 리스트로 변환
my_list = ast.literal_eval("{'key': 'value', 'number': 42}")

print(my_list[1])
