import asyncio
import traceback
import datetime
from typing import List

import aiohttp

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

        self._url = 'https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json'
        self._params = {'crtfc_key': DART_API_KEY}
        # 현재 연도를 제외하고 최근 5년을 계산하여 리스트에 저장
        # 현재 연도가 4월이 지났을 경우에는 지난 연도를 포함하여 최근 5년을 계산
        self._bsns_year_ls = [str(datetime.datetime.now().year - (i+1)) for i in range(5) if datetime.datetime.now().month > 4 or i > 0]
        self._reprt_code_ls = [
            '11011',    # 사업보고서
            '11012',    # 반기보고서
            '11013',    # 1분기보고서
            '11014'     # 3분기보고서
            ]
        self._fs_div_ls = [
            'CFS',      # 연결재무제표
            'OFS'       # 재무제표 or 별도재무제표
            ]
        self._batch_size = 300  # 한 번에 저장할 데이터 개수
        self._delay_time = 1.5  # OpenDartReader API 호출 시 딜레이 - 초 단위

    async def _delay(self):
        await asyncio.sleep(self._delay_time)

    async def _get_company_finance_info_list(self, corp_code: str, company_id: int, semaphore: asyncio.Semaphore) -> List[CollectDartFinancePydantic]:
        """OpenDartReader를 이용해 기업의 재무 정보를 가져오는 함수
        Args:
            corp_code (str): 기업의 고유번호
            semaphore (asyncio.Semaphore): asyncio.Semaphore 객체
        Returns:
            List[CollectDartFinancePydantic]: 기업의 재무 정보 리스트
        """
        async with semaphore:
            self._params['corp_code'] = corp_code
            company_finance_info_list = []
            for bsns_year in self._bsns_year_ls:
                self._params['bsns_year'] = bsns_year
                try:
                    for reprt_code in self._reprt_code_ls:
                        self._params['reprt_code'] = reprt_code
                        try:
                            for fs_div in self._fs_div_ls:
                                try:
                                    self._params['fs_div'] = fs_div
                                    async with aiohttp.ClientSession() as session:
                                        async with session.get(self._url, params=self._params) as response:
                                            if response.status != 200:
                                                err_msg = f"Error: {response.status} {response.reason}"
                                                self.logger.error(err_msg)
                                            else:
                                                data = await response.json()
                                                status = data.get('status')
                                                message = data.get('message')
                                                if data.get('status') == '000':
                                                    for info in data.get('list'):
                                                        info['company_id'] = company_id
                                                        info['fs_div'] = fs_div
                                                        info['fs_nm'] = '연결재무제표' if fs_div == 'CFS' else '별도재무제표'
                                                        company_finance_info_list.append(CollectDartFinancePydantic(**info))
                                                else:
                                                    err_msg = f"Error: {status} {message}"
                                                    self.logger.error(err_msg)
                                                    print(err_msg)
                                except Exception as e:
                                    err_msg = traceback.format_exc()
                                    self.logger.error(f"Error: {e}\n{err_msg}")
                                    continue
                        except Exception as e:
                            err_msg = traceback.format_exc()
                            self.logger.error(f"Error: {e}\n{err_msg}")
                            continue
                except Exception as e:
                    err_msg = traceback.format_exc()
                    self.logger.error(f"Error: {e}\n{err_msg}")
                    continue
            return company_finance_info_list

    async def scrape_dart_finance(self) -> None:
        """DART에서 재무 정보를 수집하는 함수"""
        semaphore = asyncio.Semaphore(10)
        tasks = [self._get_company_finance_info_list(corp_code, company_id, semaphore) for company_id, corp_code in self._compids_and_corpcodes]
        
        temp_list = []
        for task in asyncio.as_completed(tasks):
            company_finance_info_list = await task
            if company_finance_info_list:
                temp_list.extend(company_finance_info_list)
                print(f"temp_list: {len(temp_list)}")
                
                # 일정 개수 이상이 되면 데이터베이스에 저장
                if len(temp_list) >= self._batch_size:
                    self._collections_db.bulk_upsert_data_collectdartfinance(temp_list)
                    success_msg = f"Saved {len(temp_list)} data"
                    self.logger.info(success_msg)
                    print(success_msg)
                    temp_list = []  # 저장 후 리스트 초기화

        # 남은 데이터가 있다면 마지막으로 저장
        if temp_list:
            self._collections_db.bulk_upsert_data_collectdartfinance(temp_list)
            success_msg = f"Saved {len(temp_list)} data"
            self.logger.info(success_msg)
            print(success_msg)
