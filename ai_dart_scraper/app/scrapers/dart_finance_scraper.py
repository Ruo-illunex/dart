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
    def __init__(self, bsns_year:int = None, api_call_limit: int = 19900) -> None:
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
        if bsns_year:   # bsns_year 파라미터가 있으면 해당 연도만 수집
            self._bsns_year_ls = [str(bsns_year)]
        else:   # bsns_year 파라미터가 없으면 target_year와 target_year - 3 수집
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
        self._delay_time = 25  # OpenDartReader API 호출 시 딜레이 - 초 단위
        self._api_call_limit = api_call_limit   # OpenDartReader API 호출 제한 횟수
        self._api_call_count = 0    # OpenDartReader API 호출 횟수
        self._now = datetime.datetime.now() + datetime.timedelta(hours=9)   # 한국시간 기준(UTC+9)

    def _check_if_past_midnight(self):
        """현재 시간이 자정 이후인지 확인합니다."""
        now = datetime.datetime.now() + datetime.timedelta(hours=9) # 한국시간 기준(UTC+9)
        if now.day != self._now.day:    # 자정 이후
            self._now = now # 현재 시간으로 업데이트
            self._api_call_count = 0    # API 호출 횟수 초기화
            self._api_call_limit = 19900    # API 호출 제한 횟수 초기화 (OpenDartReader API 호출 제한 횟수는 하루에 20000회 이므로 100회를 여유롭게 둠)
            return True
        return False

    async def __aenter__(self):
        """세션 컨텍스트 매니저"""
        if not hasattr(self, 'session') or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """세션 컨텍스트 매니저"""
        if self.session:
            await self.session.close()

    async def _delay(self):
        """OpenDartReader API 호출 시 딜레이"""
        await asyncio.sleep(self._delay_time)

    async def _get_company_finance_info(self, session, company_id, corp_code, bsns_year, reprt_code, fs_div, semaphore, order_idx) -> None:
        """OpenDartReader API를 이용해 기업의 재무 정보를 수집하는 함수
        Args:
            session (aiohttp.ClientSession): aiohttp 세션
            company_id (str): 기업 ID   -> NewCompanyInfo 테이블의 id 컬럼
            corp_code (str): 기업 코드
            bsns_year (str): 사업연도
            reprt_code (str): 보고서 코드
            fs_div (str): 개별/연결 구분
            semaphore (asyncio.Semaphore): 동시 요청 수를 제어하는 세마포어
            order_idx (int): 순서   -> 로그 출력용
        """
        async with semaphore:   # 동시 요청 수 제어
            # 파라미터 설정 -> OpenDartReader API 문서 참고
            self._params.update({
                'corp_code': corp_code,     # 기업 코드
                'bsns_year': bsns_year,     # 사업 연도
                'reprt_code': reprt_code,   # 보고서 코드
                'fs_div': fs_div            # 재무제표 구분
            })

            # API 호출 제한 횟수를 초과하면 자정까지 기다림
            if self._api_call_count >= self._api_call_limit:
                info_msg = f"API call limit reached. Waiting until midnight"
                self._logger.info(info_msg)
                print(info_msg)
                await self._wait_until_midnight()

            # 자정 이후라면 API 호출 횟수와 제한 횟수를 초기화
            if self._check_if_past_midnight():
                info_msg = f"Past midnight. Resetting API call count and limit"
                self._logger.info(info_msg)
                print(info_msg)

            percentage = round((order_idx + 1) / self._size * 100, 2)
            info_msg = f"Start: {order_idx + 1} / {self._size} ({percentage}%) - Get company finance info of {corp_code} and bsns_year {bsns_year} and reprt_code {reprt_code} and fs_div {fs_div}"
            self._logger.info(info_msg)
            print(info_msg)

            try:
                # 데이터가 이미 있으면 로그 출력 후 다음 데이터로 넘어감
                if_exists = collections_db.check_if_exists_collectdartfinance(corp_code, bsns_year, reprt_code, fs_div)
                if if_exists:
                    info_msg = f"Skip: Already exists for corp_code {corp_code} and bsns_year {bsns_year} and reprt_code {reprt_code} and fs_div {fs_div}"
                    self._logger.info(info_msg)
                    print(info_msg)
                else:
                    self._api_call_count += 1   # API 호출 횟수 증가
                    info_msg = f"API call count: {self._api_call_count} / {self._api_call_limit}"
                    self._logger.info(info_msg)
                    print(info_msg)

                    job_done = False
                    while not job_done:
                        async with session.get(self._url, params=self._params) as response:
                            if response.status != 200:  # API 호출 실패
                                err_msg = f"Error: {response.status} {response.reason}"
                                self._logger.error(err_msg)
                            else:
                                data = await response.json()
                                status = data.get('status')
                                message = data.get('message')
                                if status == '000': # API 호출 성공
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
                                            print(err_msg)
                                    if company_finance_info_list:   # 추가할 데이터가 있으면 일괄 추가
                                        info_msg = collections_db.bulk_insert_collectdartfinance(company_finance_info_list)
                                        self._logger.info(info_msg)
                                        print(info_msg)
                                    job_done = True
                                    break
                                elif status in ['010', '011', '012', '020', '021', '800', '901']:
                                    err_msg = f"Error: {status} {message} waiting until midnight"
                                    self._logger.error(err_msg)
                                    print(err_msg)
                                    await self._wait_until_midnight()   # 자정까지 기다림
                                    self._check_if_past_midnight()  # 자정 이후인지 확인 -> API 호출 횟수와 제한 횟수 초기화
                                else:
                                    err_msg = f"Error: {status} {message} for corp_code {corp_code} and bsns_year {bsns_year} and reprt_code {reprt_code} and fs_div {fs_div}"
                                    self._logger.error(err_msg)
                                    print(err_msg)
                                    job_done = True
                                    break
            except aiohttp.ClientError as e:
                err_msg = f"ClientError: {e}"
                self._logger.error(err_msg)
            except Exception as e:
                err_msg = traceback.format_exc()
                self._logger.error(f"Unhandled exception: {err_msg}")
            finally:
                await self._delay()

    async def scrape_dart_finance(self) -> None:
        """OpenDartReader를 이용해 모든 기업의 재무 정보를 수집하는 함수"""
        async with self as scraper:
            info_msg = f"Start scraping dart finance info\n{self._size} companies will be searched"
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
        info_msg = f"Finish scraping dart finance info"
        self._logger.info(info_msg)
        print(info_msg)

    async def _wait_until_midnight(self) -> None:
        """현재 시간부터 다음 날 00:00까지 기다립니다."""
        now = datetime.datetime.now() + datetime.timedelta(hours=9) # 한국시간 기준(UTC+9)
        tomorrow = now + datetime.timedelta(days=1) # 다음 날
        midnight = datetime.datetime(year=tomorrow.year, month=tomorrow.month, day=tomorrow.day, hour=0, minute=0, second=0)    # 다음 날 00:00
        wait_seconds = (midnight - now).total_seconds() + 1 # 1초 더 기다림
        info_msg = f"API limit reached. Waiting until midnight ({wait_seconds} seconds)"
        self._logger.info(info_msg)
        print(info_msg)
        await asyncio.sleep(wait_seconds)   # 다음 날 00:00까지 기다림
