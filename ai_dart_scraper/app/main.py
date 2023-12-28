from fastapi import FastAPI

from typing import Optional

from app.common.log.log_config import setup_logger
from app.common.core.utils import get_current_datetime, make_dir
from app.models_init import *
from app.scrapers_init import *
from app.database_init import *
from app.common.db.base import BaseCollections, BaseCompanies
from app.api.dart_info_routers import router as dart_info_router
from app.api.dart_finance_routers import router as dart_finance_router
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

# 시놀로지 챗봇 설정    -> ** 추후 시놀로지 챗봇으로 알림기능 추가 **
prod_token = SYNOLOGY_CHAT['prod_token']
dev_token = SYNOLOGY_CHAT['dev_token']
test_token = SYNOLOGY_CHAT['test_token']

# DB 연결
collections_db_engine = collections_db.engine
companies_db_engine = companies_db.engine

# DB 테이블 생성
BaseCollections.metadata.create_all(bind=collections_db_engine)
BaseCompanies.metadata.create_all(bind=companies_db_engine)


app = FastAPI()
app.include_router(dart_info_router, prefix="/api/v1")
app.include_router(dart_finance_router, prefix="/api/v1")


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


@app.get("/scrape/dart_info")
async def scrape_dart_info():
    """OpenDartReader를 이용해 모든 기업의 기업 정보를 수집하는 함수"""
    scraper = DartInfoScraper()
    await scraper.scrape_dart_info()
    return {"status": "Scraping in progress..."}


# bsns_year: Optional[int] = None -> parameter가 없으면 None으로 설정
@app.get("/scrape/dart_finance")
async def scrape_dart_finance(bsns_year: Optional[int] = None):
    """OpenDartReader를 이용해 모든 기업의 재무 정보를 수집하는 함수"""
    scraper = DartFinanceScraper(bsns_year)
    await scraper.scrape_dart_finance()
    return {"status": "Scraping in progress..."}
