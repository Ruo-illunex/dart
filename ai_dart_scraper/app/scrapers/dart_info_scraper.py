import time
import asyncio
import traceback
from typing import List

import OpenDartReader
import aiohttp
from pydantic import ValidationError

from app.common.db.collections_database import CollectionsDatabase
from app.common.db.companies_database import CompaniesDatabase
from app.common.log.log_config import setup_logger
from app.config.settings import FILE_PATHS, DART_API_KEY
from app.common.core.utils import get_current_datetime, make_dir
from app.models_init import CollectDartPydantic


class DartInfoScraper:
    def __init__(self) -> None:
        file_path = FILE_PATHS["log"] + f'scrapers'
        make_dir(file_path)
        file_path += f'/dart_info_scraper_{get_current_datetime()}.log'
        self.logger = setup_logger(
            "dart_info_scraper",
            file_path
        )
        self._collections_db = CollectionsDatabase()
        self._company_id_dict = CompaniesDatabase().company_id_dict  # {corporation_num: company_id, ...}

        self._opdr = OpenDartReader(DART_API_KEY)
        self.url = 'https://opendart.fss.or.kr/api/company.json'
        self.params = {'crtfc_key': DART_API_KEY}
        self._corp_codes_ls = self._get_corp_code_list()
        self.batch_size = 100


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

    async def _delay(self):
        await asyncio.sleep(1.2)

    async def _get_company_info(self, corp_code: str, semaphore: asyncio.Semaphore) -> CollectDartPydantic:
        async with semaphore:
            try:
                self.params['corp_code'] = corp_code
                async with aiohttp.ClientSession() as session:
                    async with session.get(self.url, params=self.params) as response:
                        company_info = await response.json()

                # company_info = await asyncio.to_thread(self._opdr.company, str(corp_code))
                status = company_info.pop('status')
                message = company_info.pop('message')
                if status == '000':
                    # 기업 정보에 company_id 추가
                    company_info = self.__add_company_id_to_company_info(company_info)
                    result = CollectDartPydantic(**company_info)  # CollectDartPydantic 모델로 변환
                    print(result)
                else:
                    err_msg = f"Error: {status} {message}"
                    self.logger.error(err_msg)
                    result = None
            except ValidationError as e:
                err_msg = traceback.format_exc()
                self.logger.error(f"Error: {e}\n{err_msg}")
                result = None
            except Exception as e:
                err_msg = traceback.format_exc()
                self.logger.error(f"Error: {e}\n{err_msg}")
                result = None
            finally:
                await self._delay()

            return result

    async def _get_company_info_list(self) -> List[CollectDartPydantic]:
        """OpenDartReader를 이용해 기업 정보를 가져오는 함수
        Returns:
            List[CollectDartPydantic]: 기업 정보 리스트
        """
        semaphore = asyncio.Semaphore(5)    # 동시에 5개의 코루틴만 실행
        tasks = [self._get_company_info(corp_code, semaphore) for corp_code in self._corp_codes_ls]
        return [company_info for company_info in await asyncio.gather(*tasks) if company_info is not None]

    async def scrape_dart_info(self) -> None:
        """DART에서 기업 정보를 수집하는 함수. 100개의 데이터가 모일 때마다 데이터베이스에 저장합니다."""
        company_info_list = await self._get_company_info_list()

        temp_list = []  # 임시 저장 리스트
        for company_info in company_info_list:
            temp_list.append(company_info)

            # temp_list에 100개의 데이터가 모이면 데이터베이스에 저장
            if len(temp_list) == self.batch_size:
                self._collections_db.bulk_upsert_data_collectdart(temp_list)
                temp_list = []  # 저장 후 리스트 초기화

        # 남은 데이터가 있다면 마지막으로 저장
        if temp_list:
            self._collections_db.bulk_upsert_data_collectdart(temp_list)

