from fastapi import FastAPI
import asyncio

from app.common.db.collections_database import CollectionsDatabase
from app.common.db.companies_database import CompaniesDatabase
from app.common.db.base import BaseCollections, BaseCompanies
from app.common.log.log_config import setup_logger
from app.models_init import *
from app.scrapers_init import *
from app.common.core.utils import get_current_datetime, make_dir
from app.config.settings import FILE_PATHS, SYNOLOGY_CHAT


# 로거 설정
current_time = get_current_datetime()
file_path = FILE_PATHS["log"] + f'main_logger'
make_dir(file_path)
file_path += f'/main_{current_time}.log'
logger = setup_logger(
    "main_logger",
    file_path,
)

# 시놀로지 챗봇 설정
prod_token = SYNOLOGY_CHAT['prod_token']
dev_token = SYNOLOGY_CHAT['dev_token']
test_token = SYNOLOGY_CHAT['test_token']

# DB 연결
collections_db_engine = CollectionsDatabase().engine
companies_db_engine = CompaniesDatabase().engine

# DB 테이블 생성
BaseCollections.metadata.create_all(bind=collections_db_engine)
BaseCompanies.metadata.create_all(bind=companies_db_engine)


app = FastAPI()


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


@app.get("/scrape/dart_info")
async def scrape_dart_info():
    """OpenDartReader를 이용해 모든 기업의 기업 정보를 수집하는 함수"""
    dart_info_scraper = DartInfoScraper()
    await dart_info_scraper.scrape_dart_info()
