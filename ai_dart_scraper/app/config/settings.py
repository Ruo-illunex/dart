import os
from dotenv import load_dotenv

# 실행 환경 결정 (예: 'dev', 'prod')
env = os.getenv('ENVIRONMENT')

# .env 파일 경로 설정
env_file = '.env'

# .env 파일 로드
load_dotenv(env_file)

# DART API 키
DART_API_KEY = os.getenv('DART_API_KEY')

# 환경 변수에서 데이터베이스 설정값 읽기
username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
collections_database = os.getenv('COLLECTIONS_DB_DATABASE')
companies_database = os.getenv('COMPANIES_DB_DATABASE')

# DB URL 구성
COLLECTIONS_DB_URL = f'mysql+pymysql://{username}:{password}@{host}:{port}/{collections_database}?charset=utf8mb4'
COMPANIES_DB_URL = f'mysql+pymysql://{username}:{password}@{host}:{port}/{companies_database}?charset=utf8mb4'

# 파일 경로
FILE_PATHS = {
    'data': 'app/data/',
    'log': 'app/common/log/',
    }

# 시놀로지 챗봇 설정
SYNOLOGY_CHAT = {
    'api_url': os.getenv('SYNOLOGY_CHAT_API_URL'),
    'prod_token': os.getenv('SYNOLOGY_CHAT_PROD_TOKEN'),
    'dev_token': os.getenv('SYNOLOGY_CHAT_DEV_TOKEN'),
    'test_token': os.getenv('SYNOLOGY_CHAT_TEST_TOKEN')
}
