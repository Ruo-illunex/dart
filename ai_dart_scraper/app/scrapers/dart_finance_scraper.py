import asyncio
import traceback
import datetime

import aiohttp
from pydantic import ValidationError

from app.database_init import collections_db
from app.common.log.log_config import setup_logger
from app.config.settings import FILE_PATHS, DART_API_KEY
from app.common.core.utils import get_current_datetime, make_dir
from app.models_init import CollectDartFinancePydantic


class DartFinanceScraper:
    def __init__(self, bsns_year:int = None) -> None:
        file_path = FILE_PATHS["log"] + f'scrapers'
        make_dir(file_path)
        file_path += f'/dart_finance_scraper_{get_current_datetime()}.log'
        self._logger = setup_logger(
            "dart_finance_scraper",
            file_path
        )
        self._compids_and_corpcodes = collections_db.get_companyids_and_corpcodes()    # [(company_id, corp_code), ...]
        self._size = len(self._compids_and_corpcodes)

        self._url = 'https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json'
        self._params = {'crtfc_key': DART_API_KEY}
        self.target_year = datetime.datetime.now().year - 1 if datetime.datetime.now().month > 4 else datetime.datetime.now().year - 2
        if bsns_year:
            self._bsns_year_ls = [str(bsns_year)]
        else:
            self._bsns_year_ls = [str(self.target_year), str(self.target_year - 3)]
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
        self._delay_time = 4.5  # OpenDartReader API 호출 시 딜레이 - 초 단위

    async def __aenter__(self):
        if not hasattr(self, 'session') or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.session:
            await self.session.close()

    async def _delay(self):
        await asyncio.sleep(self._delay_time)

    async def _get_company_finance_info(self, session, company_id, corp_code, bsns_year, reprt_code, fs_div, semaphore, order_idx) -> None:
        async with semaphore:
            percentage = round((order_idx + 1) / self._size * 100, 2)
            info_msg = f"Start: {order_idx + 1} / {self._size} ({percentage}%) - Get company finance info of {corp_code} and bsns_year {bsns_year} and reprt_code {reprt_code} and fs_div {fs_div}"
            self._logger.info(info_msg)
            print(info_msg)

            self._params.update({
                'corp_code': corp_code,
                'bsns_year': bsns_year,
                'reprt_code': reprt_code,
                'fs_div': fs_div
            })

            try:
                if_exists = collections_db.check_if_exists_collectdartfinance(corp_code, bsns_year, reprt_code, fs_div)
                
                if if_exists:
                    info_msg = f"Skip: Already exists for corp_code {corp_code} and bsns_year {bsns_year} and reprt_code {reprt_code} and fs_div {fs_div}"
                    self._logger.info(info_msg)
                    print(info_msg)
                else:
                    async with session.get(self._url, params=self._params) as response:
                        if response.status != 200:
                            err_msg = f"Error: {response.status} {response.reason}"
                            self._logger.error(err_msg)
                        else:
                            data = await response.json()
                            status = data.get('status')
                            message = data.get('message')
                            if status == '000':
                                company_finance_info_list = []
                                for info in data.get('list'):
                                    try:
                                        info['company_id'] = company_id
                                        info['fs_div'] = fs_div
                                        info['fs_nm'] = '연결재무제표' if fs_div == 'CFS' else '별도재무제표'
                                        finance_info = CollectDartFinancePydantic(**info)
                                        company_finance_info_list.append(finance_info)
                                        info_msg = f"Success: Get company finance info of {info.get('corp_code')} and added to list"
                                        self._logger.info(info_msg)
                                    except ValidationError as e:
                                        err_msg = f"Validation Error for {info}: {e}"
                                        self._logger.error(err_msg)
                                if company_finance_info_list:
                                    info_msg = collections_db.bulk_insert_if_not_exists(company_finance_info_list)
                                    self._logger.info(info_msg)
                                    print(info_msg)
                            else:
                                err_msg = f"Error: {status} {message} for corp_code {corp_code} and bsns_year {bsns_year} and reprt_code {reprt_code} and fs_div {fs_div}"
                                self._logger.error(err_msg)
                                print(err_msg)
            except aiohttp.ClientError as e:
                err_msg = f"ClientError: {e}"
                self._logger.error(err_msg)
            except Exception as e:
                err_msg = traceback.format_exc()
                self._logger.error(f"Unhandled exception: {err_msg}")
            finally:
                await self._delay()

    async def scrape_dart_finance(self) -> None:
        async with self as scraper:
            info_msg = f"Start scraping dart finance info\n{len(self._size)} companies will be searched"
            self._logger.info(info_msg)
            print(info_msg)
            semaphore = asyncio.Semaphore(10)  # 동시 요청 수를 제어하는 세마포어
            tasks = []
            for i, (company_id, corp_code) in enumerate(self._compids_and_corpcodes):
                for bsns_year in self._bsns_year_ls:
                    for reprt_code in self._reprt_code_ls:
                        for fs_div in self._fs_div_ls:
                            task = asyncio.create_task(
                                self._get_company_finance_info(scraper.session, company_id, corp_code, bsns_year, reprt_code, fs_div, semaphore, i),
                                name=f"{company_id}_{bsns_year}_{reprt_code}_{fs_div}")
                            tasks.append(task)
            await asyncio.gather(*tasks)
