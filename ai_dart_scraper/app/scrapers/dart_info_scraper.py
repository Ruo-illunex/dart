import time
import asyncio
import traceback
from typing import List

import OpenDartReader
from pydantic import ValidationError

from app.common.db.collections_database import CollectionsDatabase
from app.common.db.companies_database import CompaniesDatabase
from app.common.log.log_config import setup_logger
from app.config.settings import FILE_PATHS, DART_API_KEY
from app.common.core.utils import get_current_datetime, make_dir
from app.models_init import CollectDartPydantic


class DartInfoScraper:
    def __init__(self) -> None:
        file_path = FILE_PATHS["log"] + f'scrapers/dart_info_{get_current_datetime()}.log'
        make_dir(file_path)
        self.logger = setup_logger(
            "dart_info_scraper",
            file_path
        )
        self._collections_db = CollectionsDatabase()
        self._company_id_dict = CompaniesDatabase().company_id_dict  # {corporation_num: company_id, ...}

        self._opdr = OpenDartReader(DART_API_KEY)
        self._corp_codes_ls = self._get_corp_code_list()


    def _get_corp_code_list(self) -> list:
        """OpenDartReader를 이용해 모든 기업의 고유번호 리스트를 가져오는 함수
        Returns:
            list: 고유번호 리스트
        """
        return self._opdr.corp_codes['corp_code'].tolist()

    def __add_company_id_to_company_info(self, company_info: dict) -> dict:
        """기업 정보에 company_id를 추가하는 함수
        Args:
            company_info (dict): 기업 정보
        Returns:
            dict: 기업 정보
        """
        # 기업 정보에 company_id 추가: bizr_no를 키로 사용
        company_info['company_id'] = self._company_id_dict.get(company_info.get('bizr_no'))
        return company_info

    async def _get_company_info(self, corp_code: str) -> CollectDartPydantic:
        """OpenDartReader를 이용해 기업 정보를 가져오는 함수
        Args:
            corp_code (str): OpenDartReader를 이용해 가져온 기업의 고유번호
        Returns:
            dict: 기업 정보
        """
        try:
            # OpenDartReader를 이용해 기업 정보 가져오기 - 비동기 처리
            company_info = await asyncio.to_thread(self._opdr.company, str(corp_code))
            status = company_info['status'].pop()
            message = company_info['message'].pop()
            if status == '000':
                # 기업 정보에 company_id 추가
                company_info = self.__add_company_id_to_company_info(company_info)
                return CollectDartPydantic(**company_info)  # CollectDartPydantic 모델로 변환
            else:
                err_msg = f"Error: {status} {message}"
                self.logger.error(err_msg)
                return None
        except ValidationError as e:
            err_msg = traceback.format_exc()
            self.logger.error(f"Error: {e}\n{err_msg}")
            return None
        except Exception as e:
            err_msg = traceback.format_exc()
            self.logger.error(f"Error: {e}\n{err_msg}")
            return None

    async def _get_company_info_list(self) -> List[CollectDartPydantic]:
        """OpenDartReader를 이용해 기업 정보를 가져오는 함수
        Returns:
            List[CollectDartPydantic]: 기업 정보 리스트
        """
        tasks = [self._get_company_info(corp_code) for corp_code in self._corp_codes_ls]
        return [company_info for company_info in await asyncio.gather(*tasks) if company_info is not None]

    def scrape_dart_info(self) -> None:
        """DART에서 기업 정보를 수집하는 함수"""
        company_info_list = asyncio.run(self._get_company_info_list())
        self._collections_db.bulk_upsert_data_collectdart(company_info_list)
