import asyncio
import traceback

import aiohttp
from pydantic import ValidationError

from app.common.db.collections_database import CollectionsDatabase
from app.common.db.companies_database import CompaniesDatabase
from app.common.log.log_config import setup_logger
from app.config.settings import FILE_PATHS, DART_API_KEY
from app.common.core.utils import get_current_datetime, make_dir, get_corp_codes
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
        self._companies_db = CompaniesDatabase()
        self._company_id_dict = self._companies_db.company_id_dict  # {corporation_num: company_id, ...}

        self._url = 'https://opendart.fss.or.kr/api/company.json'
        self._params = {'crtfc_key': DART_API_KEY}
        self._corp_codes_ls = self._get_corp_code_list()
        self._batch_size = 100  # 한 번에 저장할 데이터 개수
        self._delay_time = 1.5  # OpenDartReader API 호출 시 딜레이 - 초 단위

    # aiohttp.ClientSession을 인스턴스 수준에서 초기화
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.session.close()

    def _get_corp_code_list(self) -> list:
        """OpenDartReader를 이용해 모든 기업의 고유번호 리스트를 가져오는 함수
        Returns:
            list: 고유번호 리스트
        """
        corp_codes = get_corp_codes()
        return corp_codes['corp_code'].tolist()

    def __add_company_id_to_company_info(self, company_info: dict) -> dict:
        """기업 정보에 company_id를 추가하는 함수
        Args:
            company_info (dict): 기업 정보
        Returns:
            dict: 기업 정보
        """
        # 기업 정보에 company_id 추가: bizr_no를 키로 사용
        company_info['company_id'] = self._company_id_dict.get(company_info.get('jurir_no'))
        return company_info

    async def _delay(self):
        await asyncio.sleep(self._delay_time)

    async def _get_company_info(self, corp_code: str, semaphore: asyncio.Semaphore) -> CollectDartPydantic:
        async with semaphore:
            try:
                self._params['corp_code'] = corp_code
                async with self.session.get(self._url, params=self._params) as response:
                    if response.status != 200:
                        err_msg = f"Error: {response.status} {response.reason}"
                        self.logger.error(err_msg)
                        result = None
                    else:
                        company_info = await response.json()
                        info_msg = f"Success: Get company info of {company_info.get('corp_name')}"
                        self.logger.info(info_msg)
                        print(info_msg)

                status = company_info.pop('status')
                message = company_info.pop('message')
                if status == '000':
                    # 기업 정보에 company_id 추가
                    company_info = self.__add_company_id_to_company_info(company_info)
                    result = CollectDartPydantic(**company_info)  # CollectDartPydantic 모델로 변환
                    info_msg = f"Success: Transformed company info of {company_info.get('corp_name')} and added company_id {company_info.get('company_id')}"
                    self.logger.info(info_msg)
                    print(info_msg)
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

    async def scrape_dart_info(self) -> None:
        """DART에서 기업 정보를 수집하는 함수. asyncio.Semaphore를 이용해 동시에 5개의 코루틴만 실행"""
        async with aiohttp.ClientSession() as self.session:  # aiohttp.ClientSession을 사용하여 세션 관리
            semaphore = asyncio.Semaphore(5)  # 동시에 5개의 코루틴만 실행
            tasks = [self._get_company_info(corp_code, semaphore) for corp_code in self._corp_codes_ls]

            temp_list = []  # 임시 저장 리스트
            for task in asyncio.as_completed(tasks):
                company_info = await task
                if company_info:
                    temp_list.append(company_info)
                    print(f"temp_list: {len(temp_list)}")

                    # temp_list에 100개의 데이터가 모이면 데이터베이스에 저장
                    if len(temp_list) == self._batch_size:
                        self._collections_db.bulk_upsert_data_collectdart(temp_list)
                        success_msg = f"Saved {len(temp_list)} data"
                        self.logger.info(success_msg)
                        print(success_msg)
                        temp_list = []  # 저장 후 리스트 초기화

            # 남은 데이터가 있다면 마지막으로 저장
            if temp_list:
                self._collections_db.bulk_upsert_data_collectdart(temp_list)
                success_msg = f"Saved {len(temp_list)} data"
                self.logger.info(success_msg)
                print(success_msg)
