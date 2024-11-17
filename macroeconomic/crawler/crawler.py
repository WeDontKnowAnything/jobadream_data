# 필요한 모듈 임포트
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
import time
from bs4 import BeautifulSoup
import pandas as pd
import random
from selenium.webdriver.chrome.options import Options

from selenium.webdriver.chrome.service import Service

def create_driver():
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    
    # ChromeDriver 경로를 지정하지 않습니다.
    driver = webdriver.Chrome(options=chrome_options)
    
    return driver

# CSV 파일 불러오기
df_companies = pd.read_csv('상장법인목록_2024_08_09.csv', encoding='utf-8')

# '회사명' 열만 추출
company_names = df_companies['회사명'].tolist()

# 크롬드라이버 실행
driver = webdriver.Chrome()

# 크롬 드라이버에 URL 주소 넣고 실행
driver.get('https://insight.wanted.co.kr/')
time.sleep(3)

# xpath를 사용하여 요소를 가져옴
element = driver.find_element(By.XPATH, '//*[@id="__next"]/div[1]/nav/div/div[2]/a')
element.click()
time.sleep(random.uniform(1, 3)) 

# 전체 데이터를 저장할 리스트 생성
data_list = []

# 첫번째 회사 검색 및 처리
search_box = driver.find_element(By.XPATH, '//*[@id="__next"]/div[2]/div/div/form/div/div/input')
search_box.send_keys(company_names[0])
search_box.send_keys(Keys.RETURN)
time.sleep(random.uniform(1, 3)) 

company_link = driver.find_element(By.XPATH, '//*[@id="__next"]/div[2]/div/div/form/div[2]/ul/li/a')
href = company_link.get_attribute('href')

# 링크로 이동
driver.get(href)
time.sleep(random.uniform(1, 3)) 

# 데이터가 들어있는 부분을 BeautifulSoup으로 가져오기
soup = BeautifulSoup(driver.page_source, 'html.parser')

# 첫 번째 회사의 데이터를 추출하여 리스트에 추가
data = {
    "회사": company_names[0],
    "표준산업분류": soup.find('dt', string='표준산업분류').find_next('dd').text if soup.find('dt', string='표준산업분류') else '',
    "연혁": soup.find('dt', string='연혁').find_next('dd').text if soup.find('dt', string='연혁') else '',
    "매출액": soup.find('dt', string='매출액').find_next('dd').text if soup.find('dt', string='매출액') else '',
    "기업유형": soup.find('dt', string='기업유형').find_next('dd').text if soup.find('dt', string='기업유형') else '',
    "평균연봉": soup.find('dt', string='평균연봉').find_next('dd').text if soup.find('dt', string='평균연봉') else '',
    "홈페이지": soup.find('dt', string='홈페이지').find_next('dd').text if soup.find('dt', string='홈페이지') else '',
    "고용보험 사업장 수": soup.find('dt', string='고용보험 사업장 수').find_next('dd').text if soup.find('dt', string='고용보험 사업장 수') else '',
    "고용보험 가입 사원수": soup.find('dt', string='고용보험 가입 사원수').find_next('dd').text if soup.find('dt', string='고용보험 가입 사원수') else '',
    "국민연금 가입 사원수": soup.find('dt', string='국민연금 가입 사원수').find_next('dd').text if soup.find('dt', string='국민연금 가입 사원수') else '',
    "퇴사/입사 (1년)": soup.find('dt', string='퇴사/입사 (1년)').find_next('dd').text if soup.find('dt', string='퇴사/입사 (1년)') else ''
}

# 리스트에 첫 번째 데이터 추가
data_list.append(data)

# 나머지 회사 검색 및 처리
for company_name in company_names[1:]:
    time.sleep(1)
    
    try:
        # 새 검색 버튼 클릭
        new_search = driver.find_element(By.XPATH, '//*[@id="__next"]/div[1]/div[2]/nav/aside/ul/li[1]/button')
        new_search.click()
        time.sleep(random.uniform(1, 3)) 
        
        # 회사명 입력 및 검색 실행
        search_input = driver.find_element(By.XPATH, '//*[@id="nav_searchbar"]/div/div[2]/div/form/input')
        search_input.send_keys(company_name)
        search_input.send_keys(Keys.RETURN)
        time.sleep(random.uniform(1, 3)) 

        # 검색 결과 링크 클릭
        company_link = driver.find_element(By.XPATH, '//*[@id="search_tabpanel_overview"]/div/div[2]/ul/li/a')
        href = company_link.get_attribute('href')

        # 페이지 이동
        driver.get(href)
        time.sleep(random.uniform(1, 3)) 

        # 페이지에서 데이터 추출
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        time.sleep(random.uniform(1, 3)) 
        data = {
            "회사": company_name,
            "표준산업분류": soup.find('dt', string='표준산업분류').find_next('dd').text if soup.find('dt', string='표준산업분류') else '',
            "연혁": soup.find('dt', string='연혁').find_next('dd').text if soup.find('dt', string='연혁') else '',
            "매출액": soup.find('dt', string='매출액').find_next('dd').text if soup.find('dt', string='매출액') else '',
            "기업유형": soup.find('dt', string='기업유형').find_next('dd').text if soup.find('dt', string='기업유형') else '',
            "평균연봉": soup.find('dt', string='평균연봉').find_next('dd').text if soup.find('dt', string='평균연봉') else '',
            "홈페이지": soup.find('dt', string='홈페이지').find_next('dd').text if soup.find('dt', string='홈페이지') else '',
            "고용보험 사업장 수": soup.find('dt', string='고용보험 사업장 수').find_next('dd').text if soup.find('dt', string='고용보험 사업장 수') else '',
            "고용보험 가입 사원수": soup.find('dt', string='고용보험 가입 사원수').find_next('dd').text if soup.find('dt', string='고용보험 가입 사원수') else '',
            "국민연금 가입 사원수": soup.find('dt', string='국민연금 가입 사원수').find_next('dd').text if soup.find('dt', string='국민연금 가입 사원수') else '',
            "퇴사/입사 (1년)": soup.find('dt', string='퇴사/입사 (1년)').find_next('dd').text if soup.find('dt', string='퇴사/입사 (1년)') else ''
        }
        

        # 리스트에 데이터 추가
        data_list.append(data)
        
    except Exception:
        print(f"No results found for {company_name}, skipping.")
        continue  # 검색 결과가 없으면 다음으로 넘어감

# 리스트를 데이터프레임으로 변환
df_total = pd.DataFrame(data_list)

# 최종 데이터프레임을 CSV 파일로 저장
df_total.to_csv('new_company_data.csv', encoding='utf-8-sig', index=False)

# 크롬 드라이버 종료
driver.quit()
