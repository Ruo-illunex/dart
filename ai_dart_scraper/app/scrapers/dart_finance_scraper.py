import asyncio
import traceback

import OpenDartReader
import aiohttp
from pydantic import ValidationError

from app.common.db.collections_database import CollectionsDatabase
from app.common.db.companies_database import CompaniesDatabase
from app.common.log.log_config import setup_logger
from app.config.settings import FILE_PATHS, DART_API_KEY
from app.common.core.utils import get_current_datetime, make_dir
from app.models_init import CollectDartFinancePydantic


class DartInfoScraper:
    def __init__(self) -> None:
        file_path = FILE_PATHS["log"] + f'scrapers'
        make_dir(file_path)
        file_path += f'/dart_finance_scraper_{get_current_datetime()}.log'
        self.logger = setup_logger(
            "dart_finance_scraper",
            file_path
        )
        self._collections_db = CollectionsDatabase()
        self._companies_db = CompaniesDatabase()
        self._compids_and_corpcodes = self._collections_db.compids_and_corpcodes    # [(company_id, corp_code), ...]

        self._opdr = OpenDartReader(DART_API_KEY)
        self.url = 'https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json'
        self.params = {'crtfc_key': DART_API_KEY}
        self.batch_size = 100
