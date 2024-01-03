import traceback
from typing import List, Optional

import pandas as pd

from app.common.log.log_config import setup_logger
from app.config.settings import FILE_PATHS
from app.common.core.utils import get_current_datetime, make_dir, get_current_date
from app.models_init import NewCompanyFinancePydantic
from app.database_init import companies_db


class DartFinancePreprocessing:
    def __init__(self):
        file_path = FILE_PATHS["log"] + 'preprocessing'
        make_dir(file_path)
        file_path += f'/dart_finance_preprocessing_{get_current_datetime()}.log'
        self._logger = setup_logger(
            "dart_finance_preprocessing",
            file_path
        )

    def _cal_ids(self, company_id: str) -> tuple:
        """기업 ID로 사업자등록번호와 법인등록번호를 조회하는 함수
        Args:
            company_id (str): 기업 ID
        Returns:
            tuple: (사업자등록번호, 법인등록번호)
        """
        data = companies_db.query_companies(company_id=company_id)
        biz_num = data.get('biz_num')
        corporation_num = data.get('corporation_num')
        illu_id = data.get('illu_id')
        return biz_num, corporation_num, illu_id

    def _preprocess_values(self, value: Optional[str]) -> str:
        """값을 전처리하는 함수 : 1000000000 -> 1000000
        Args:
            value (Optional[str]): 값
        Returns:
            str: 전처리된 값
        """
        try:
            if not value:
                return ''
            return value[:-3]
        except Exception as e:
            err_msg = traceback.format_exc()
            self._logger.error(err_msg)
            self._logger.error(f"Error: {e}")
            return ''

    def _search_values(self, df: pd.DataFrame, account_nm: str = None, sj_div: str = None, alt_account_nm_ls: list = [], account_id: str = None, alt_account_id_ls: list = [], alt_sj_div_ls: list = []) -> tuple:
        """항목명으로 해당 항목의 값들을 찾는 함수
        Args:
            df (pd.DataFrame): 재무 정보 데이터프레임
            account_nm (str): 항목명    -> account_nm과 account_id 중 하나만 입력해야 함
            sj_div (str): 재무제표구분 -> BS: 재무상태표, CIS: 포괄손익계산서, IS: 손익계산서, CF: 현금흐름표
            alt_account_nm_ls (list): 대체 항목명 리스트    -> account_nm의 결과가 없을 경우 대체 항목명으로 검색
            account_id (str): 항목 ID   -> account_nm과 account_id 중 하나만 입력해야 함
            alt_account_id_ls (list): 대체 항목 ID 리스트   -> account_id의 결과가 없을 경우 대체 항목 ID로 검색
            alt_sj_div_ls (list): 대체 재무제표구분 리스트   -> sj_div의 결과가 없을 경우 대체 재무제표구분으로 검색
        Returns:
            tuple: (당기, 전기, 전전기)
        """
        thstrm_amount, frmtrm_amount, bfefrmtrm_amount = None, None, None
        if account_id:
            cond = (df.account_id == account_id)
        else:
            cond = (df.account_nm == account_nm)
        if sj_div:
            cond = cond & (df.sj_div == sj_div)
        try:
            df_ = df[cond]
            if not df_.empty:
                thstrm_amount = df_.thstrm_amount.values[0]
                frmtrm_amount = df_.frmtrm_amount.values[0]
                bfefrmtrm_amount = df_.bfefrmtrm_amount.values[0]
                return self._preprocess_values(thstrm_amount), self._preprocess_values(frmtrm_amount), self._preprocess_values(bfefrmtrm_amount)
            else:
                if alt_account_nm_ls:
                    for alt_account_nm in alt_account_nm_ls:
                        thstrm_amount, frmtrm_amount, bfefrmtrm_amount = self._search_values(
                            df, account_nm=alt_account_nm, sj_div=sj_div
                            )
                        if thstrm_amount != '':
                            return thstrm_amount, frmtrm_amount, bfefrmtrm_amount
                if alt_account_id_ls:
                    for alt_account_id in alt_account_id_ls:
                        thstrm_amount, frmtrm_amount, bfefrmtrm_amount = self._search_values(
                            df, sj_div=sj_div, account_id=alt_account_id
                            )
                        if thstrm_amount != '':
                            return thstrm_amount, frmtrm_amount, bfefrmtrm_amount
                if alt_sj_div_ls:
                    for alt_sj_div in alt_sj_div_ls:
                        thstrm_amount, frmtrm_amount, bfefrmtrm_amount = self._search_values(
                            df, sj_div=alt_sj_div, account_nm=account_nm, account_id=account_id
                            )
                        if thstrm_amount != '':
                            return thstrm_amount, frmtrm_amount, bfefrmtrm_amount
                return '', '', ''
        except Exception as e:
            err_msg = traceback.format_exc()
            self._logger.error(err_msg)
            self._logger.error(f"Error: {e}")
            return '', '', ''

    def _cal_financial_dept_ratio(self, capital_total: str, dept_total: str) -> str:
        """부채비율을 계산하는 함수
        Args:
            capital_total (str): 자본총계
            dept_total (str): 부채총계
        Returns:
            str: 부채비율
        """
        financial_dept_ratio = ''
        try:
            assert capital_total != '', '자본총계가 없습니다.'
            assert dept_total != '', '부채총계가 없습니다.'
            financial_dept_ratio = str(round(int(dept_total) / int(capital_total) * 100, 4))
        except AssertionError as e:
            err_msg = traceback.format_exc()
            self._logger.error(err_msg)
            self._logger.error(f"Error: {e}")
            financial_dept_ratio = ''
        except ZeroDivisionError:
            err_msg = traceback.format_exc()
            self._logger.error(err_msg)
            self._logger.error(f"Error: {e}")
            financial_dept_ratio = ''
        except Exception as e:
            err_msg = traceback.format_exc()
            self._logger.error(err_msg)
            self._logger.error(f"Error: {e}")
            financial_dept_ratio = ''
        finally:
            return financial_dept_ratio

    def _cal_net_worth(self, asset_total: str, dept_total: str) -> str:
        """순자산총계를 계산하는 함수
        Args:
            asset_total (str): 자산총계
            dept_total (str): 부채총계
        Returns:
            str: 순자산총계
        """
        networth_total = ''
        try:
            assert asset_total != '', '자산총계가 없습니다.'
            assert dept_total != '', '부채총계가 없습니다.'
            networth_total = str(int(asset_total) - int(dept_total))
        except AssertionError as e:
            err_msg = traceback.format_exc()
            self._logger.error(err_msg)
            self._logger.error(f"Error: {e}")
        except Exception as e:
            err_msg = traceback.format_exc()
            self._logger.error(err_msg)
            self._logger.error(f"Error: {e}")
        finally:
            return networth_total

    def _cal_quick_asset(self, current_asset: str, inventories_asset: str) -> str:
        """당좌자산을 계산하는 함수
        Args:
            current_asset (str): 유동자산
            inventories_asset (str): 재고자산
        Returns:
            str: 당좌자산
        """
        quick_asset = ''
        try:
            assert current_asset != '', '유동자산이 없습니다.'
            assert inventories_asset != '', '재고자산이 없습니다.'
            quick_asset = str(int(current_asset) - int(inventories_asset))
        except AssertionError as e:
            err_msg = traceback.format_exc()
            self._logger.error(err_msg)
            self._logger.error(f"Error: {e}")
        except Exception as e:
            err_msg = traceback.format_exc()
            self._logger.error(err_msg)
            self._logger.error(f"Error: {e}")
        finally:
            return quick_asset

    def _cal_net_working_capital(self, current_asset: str, current_liabilities: str) -> str:
        """순운전자본을 계산하는 함수
        Args:
            current_asset (str): 유동자산
            current_liabilities (str): 유동부채
        Returns:
            str: 순운전자본
        """
        net_working_capital = ''
        try:
            assert current_asset != '', '유동자산이 없습니다.'
            assert current_liabilities != '', '유동부채가 없습니다.'
            net_working_capital = str(int(current_asset) - int(current_liabilities))
        except AssertionError as e:
            err_msg = traceback.format_exc()
            self._logger.error(err_msg)
            self._logger.error(f"Error: {e}")
        except Exception as e:
            err_msg = traceback.format_exc()
            self._logger.error(err_msg)
            self._logger.error(f"Error: {e}")
        finally:
            return net_working_capital

    def _parse_company_finance(self, _df: pd.DataFrame, company_id: str, biz_num: str, corporation_num: str, illu_id: str, year: str, fs: str, create_date: str, update_date: str):
        """기업 재무 정보를 파싱하는 함수
        Args:
            _df (pd.DataFrame): 재무 정보 데이터프레임
            company_id (str): 기업 ID
            biz_num (str): 사업자등록번호
            corporation_num (str): 법인등록번호
            illu_id (str): 일루넥스 ID
            year (str): 연도
            fs (str): 재무제표구분
            create_date (str): 생성일
            update_date (str): 수정일
        Returns:
            tuple: (당기, 전기, 전전기)
        """
        thstrm_company_finance, frmtrm_company_finance, bfefrmtrm_company_finance = None, None, None

        df = _df.loc[(_df['bsns_year'] == year) & (_df['fs_div'] == fs)]

        # 임시 변수 -> 당기, 전기, 전전기 재무 정보를 담을 딕셔너리
        _ths = {}
        _frm = {}
        _bfe = {}

        financial_decide_code = df.fs_div.values[0]  # 재무제표구분
        financial_decide_desc = df.fs_nm.values[0]  # 재무제표명
        thstrm_year, frmtrm_year, bfefrmtrm_year = year, str(int(year)-1), str(int(year)-2) # 당기, 전기, 전전기 연도

        _ths['sales'], _frm['sales'], _bfe['sales'] = self._search_values(
            df,
            account_id='ifrs-full_Revenue',
            alt_account_nm_ls=['매출액']
            )   # 매출액

        _ths['sales_cost'], _frm['sales_cost'], _bfe['sales_cost'] = self._search_values(
            df,
            account_id='ifrs-full_CostOfSales',
            alt_account_nm_ls=['매출원가']
            )   # 매출원가

        _ths['operating_profit'], _frm['operating_profit'], _bfe['operating_profit'] = self._search_values(
            df,
            account_id='dart_OperatingIncomeLoss',
            alt_account_nm_ls=['영업이익']
            )   # 영업이익

        _ths['net_profit'], _frm['net_profit'], _bfe['net_profit'] = self._search_values(
            df,
            sj_div='CIS',
            account_id='ifrs-full_ProfitLoss',
            alt_sj_div_ls=['IS'],
            alt_account_nm_ls=['당기순이익', '당기순이익(손실)']
            )   # 당기순이익

        _ths['capital_amount'], _frm['capital_amount'], _bfe['capital_amount'] = self._search_values(
            df,
            account_nm='자본금',
            sj_div='BS'
            )   # 자본금

        _ths['capital_total'], _frm['capital_total'], _bfe['capital_total'] = self._search_values(
            df,
            account_nm='자본총계',
            sj_div='BS'
            )   # 자본총계

        _ths['dept_total'], _frm['dept_total'], _bfe['dept_total'] = self._search_values(
            df,
            account_nm='부채총계',
            sj_div='BS'
            )   # 부채총계

        _ths['assets_total'], _frm['assets_total'], _bfe['assets_total'] = self._search_values(
            df,
            account_nm='자산총계',
            sj_div='BS'
            )   # 자산총계

        _ths['comprehensive_income'], _frm['comprehensive_income'], _bfe['comprehensive_income'] = self._search_values(
            df,
            sj_div='CIS',
            account_id='ifrs-full_ComprehensiveIncome',
            alt_account_id_ls=['ifrs_ComprehensiveIncome'],
            alt_account_nm_ls=['총포괄손익']
            )   # 총포괄손익

        _ths['tangible_asset'], _frm['tangible_asset'], _bfe['tangible_asset'] = self._search_values(
            df,
            account_id='ifrs-full_PropertyPlantAndEquipment',
            alt_account_id_ls=['ifrs_PropertyPlantAndEquipment'],
            alt_account_nm_ls=['유형자산']
            )   # 유형자산

        _ths['none_tangible_asset'], _frm['none_tangible_asset'], _bfe['none_tangible_asset'] = self._search_values(
            df,
            account_id='ifrs-full_IntangibleAssetsOtherThanGoodwill',
            alt_account_id_ls=['dart_OtherIntangibleAssetsGross', 'dart_GoodwillGross'],
            alt_account_nm_ls=['무형자산']
            )   # 무형자산

        _ths['current_asset'], _frm['current_asset'], _bfe['current_asset'] = self._search_values(
            df,
            account_id='ifrs-full_CurrentAssets',
            alt_account_id_ls=['ifrs_CurrentAssets'],
            alt_account_nm_ls=['유동자산']
            )   # 유동자산

        _ths['none_current_asset'], _frm['none_current_asset'], _bfe['none_current_asset'] = self._search_values(
            df,
            account_id='ifrs-full_NoncurrentAssets',
            alt_account_id_ls=['ifrs_NonCurrentAssets'],
            alt_account_nm_ls=['비유동자산']
            )   # 비유동자산

        _ths['current_liabilities'], _frm['current_liabilities'], _bfe['current_liabilities'] = self._search_values(
            df,
            account_id='ifrs-full_CurrentLiabilities',
            alt_account_id_ls=['ifrs_CurrentLiabilities'],
            alt_account_nm_ls=['유동부채']
            )   # 유동부채

        _ths['inventories_asset'], _frm['inventories_asset'], _bfe['inventories_asset'] = self._search_values(
            df,
            account_id='ifrs-full_Inventories',
            alt_account_id_ls=['ifrs_Inventories'],
            alt_account_nm_ls=['재고자산']
            )   # 재고자산

        _ths['accounts_payable'], _frm['accounts_payable'], _bfe['accounts_payable'] = self._search_values(
            df,
            sj_div='BS',
            account_id='ifrs-full_TradeAndOtherCurrentPayables',
            alt_account_id_ls=['dart_ShortTermTradePayables'],
            alt_account_nm_ls=['매입채무', '단기매입채무']
            )   # 매입채무

        _ths['trade_receivable'], _frm['trade_receivable'], _bfe['trade_receivable'] = self._search_values(
            df,
            sj_div='BS',
            account_id='ifrs-full_TradeAndOtherCurrentReceivables',
            alt_account_id_ls=['dart_ShortTermTradeReceivable'],
            alt_account_nm_ls=['매출채권', '단기매출채권']
            )   # 매출채권

        _ths['short_term_loan'], _frm['short_term_loan'], _bfe['short_term_loan'] = self._search_values(
            df,
            sj_div='BS',
            account_id='ifrs-full_ShorttermBorrowings',
            alt_account_nm_ls=['단기차입금']
            )   # 단기차입금

        _ths['admin_expenses'], _frm['admin_expenses'], _bfe['admin_expenses'] = self._search_values(
            df,
            account_id='dart_TotalSellingGeneralAdministrativeExpenses'
            )   # 판매비와 관리비

        # 부채비율
        _ths['financial_debt_ratio'] = self._cal_financial_dept_ratio(_ths['capital_total'], _ths['dept_total'])
        _frm['financial_debt_ratio'] = self._cal_financial_dept_ratio(_frm['capital_total'], _frm['dept_total'])
        _bfe['financial_debt_ratio'] = self._cal_financial_dept_ratio(_bfe['capital_total'], _bfe['dept_total'])

        # 순자산총계
        _ths['net_worth'] = self._cal_net_worth(_ths['assets_total'], _ths['dept_total'])
        _frm['net_worth'] = self._cal_net_worth(_frm['assets_total'], _frm['dept_total'])
        _bfe['net_worth'] = self._cal_net_worth(_bfe['assets_total'], _bfe['dept_total'])

        # 당좌자산
        _ths['quick_asset'] = self._cal_quick_asset(_ths['current_asset'], _ths['inventories_asset'])
        _frm['quick_asset'] = self._cal_quick_asset(_frm['current_asset'], _frm['inventories_asset'])
        _bfe['quick_asset'] = self._cal_quick_asset(_bfe['current_asset'], _bfe['inventories_asset'])

        # 순운전자본
        _ths['networking_capital'] = self._cal_net_working_capital(_ths['current_asset'], _ths['current_liabilities'])
        _frm['networking_capital'] = self._cal_net_working_capital(_frm['current_asset'], _frm['current_liabilities'])
        _bfe['networking_capital'] = self._cal_net_working_capital(_bfe['current_asset'], _bfe['current_liabilities'])

        # 재무 정보 파싱 결과를 NewCompanyFinancePydantic 형태로 저장
        if set(_ths.values()) != {''}:  # _ths.values()에 값이 하나라도 있으면
            thstrm_company_finance = NewCompanyFinancePydantic(
                companyId=company_id,
                bizNum=biz_num,
                corporationNum=corporation_num,
                illuId=illu_id,
                acctDt=thstrm_year,
                financialDecideCode=financial_decide_code,
                financialDecideDesc=financial_decide_desc,
                sales=_ths['sales'],
                salesCost=_ths['sales_cost'],
                operatingProfit=_ths['operating_profit'],
                netProfit=_ths['net_profit'],
                capitalAmount=_ths['capital_amount'],
                capitalTotal=_ths['capital_total'],
                debtTotal=_ths['dept_total'],
                assetTotal=_ths['assets_total'],
                comprehensiveIncome=_ths['comprehensive_income'],
                financialDebtRatio=_ths['financial_debt_ratio'],
                tangibleAsset=_ths['tangible_asset'],
                nonTangibleAsset=_ths['none_tangible_asset'],
                currentAsset=_ths['current_asset'],
                nonCurrentAsset=_ths['none_current_asset'],
                currentLiabilities=_ths['current_liabilities'],
                netWorth=_ths['net_worth'],
                quickAsset=_ths['quick_asset'],
                inventoriesAsset=_ths['inventories_asset'],
                accountsPayable=_ths['accounts_payable'],
                tradeReceivable=_ths['trade_receivable'],
                shortTermLoan=_ths['short_term_loan'],
                netWorkingCapital=_ths['networking_capital'],
                sellingGeneralAdministrativeExpenses=_ths['admin_expenses'],
                createDate=create_date,
                updateDate=update_date
            )   # 당기 재무 정보

        if set(_frm.values()) != {''}:  # _frm.values()에 값이 하나라도 있으면
            frmtrm_company_finance = NewCompanyFinancePydantic(
                companyId=company_id,
                bizNum=biz_num,
                corporationNum=corporation_num,
                illuId=illu_id,
                acctDt=frmtrm_year,
                financialDecideCode=financial_decide_code,
                financialDecideDesc=financial_decide_desc,
                sales=_frm['sales'],
                salesCost=_frm['sales_cost'],
                operatingProfit=_frm['operating_profit'],
                netProfit=_frm['net_profit'],
                capitalAmount=_frm['capital_amount'],
                capitalTotal=_frm['capital_total'],
                debtTotal=_frm['dept_total'],
                assetTotal=_frm['assets_total'],
                comprehensiveIncome=_frm['comprehensive_income'],
                financialDebtRatio=_frm['financial_debt_ratio'],
                tangibleAsset=_frm['tangible_asset'],
                nonTangibleAsset=_frm['none_tangible_asset'],
                currentAsset=_frm['current_asset'],
                nonCurrentAsset=_frm['none_current_asset'],
                currentLiabilities=_frm['current_liabilities'],
                netWorth=_frm['net_worth'],
                quickAsset=_frm['quick_asset'],
                inventoriesAsset=_frm['inventories_asset'],
                accountsPayable=_frm['accounts_payable'],
                tradeReceivable=_frm['trade_receivable'],
                shortTermLoan=_frm['short_term_loan'],
                netWorkingCapital=_frm['networking_capital'],
                sellingGeneralAdministrativeExpenses=_frm['admin_expenses'],
                createDate=create_date,
                updateDate=update_date
            )   # 전기 재무 정보

        if set(_bfe.values()) != {''}:  # _bfe.values()에 값이 하나라도 있으면
            bfefrmtrm_company_finance = NewCompanyFinancePydantic(
                companyId=company_id,
                bizNum=biz_num,
                corporationNum=corporation_num,
                illuId=illu_id,
                acctDt=bfefrmtrm_year,
                financialDecideCode=financial_decide_code,
                financialDecideDesc=financial_decide_desc,
                sales=_bfe['sales'],
                salesCost=_bfe['sales_cost'],
                operatingProfit=_bfe['operating_profit'],
                netProfit=_bfe['net_profit'],
                capitalAmount=_bfe['capital_amount'],
                capitalTotal=_bfe['capital_total'],
                debtTotal=_bfe['dept_total'],
                assetTotal=_bfe['assets_total'],
                comprehensiveIncome=_bfe['comprehensive_income'],
                financialDebtRatio=_bfe['financial_debt_ratio'],
                tangibleAsset=_bfe['tangible_asset'],
                nonTangibleAsset=_bfe['none_tangible_asset'],
                currentAsset=_bfe['current_asset'],
                nonCurrentAsset=_bfe['none_current_asset'],
                currentLiabilities=_bfe['current_liabilities'],
                netWorth=_bfe['net_worth'],
                quickAsset=_bfe['quick_asset'],
                inventoriesAsset=_bfe['inventories_asset'],
                accountsPayable=_bfe['accounts_payable'],
                tradeReceivable=_bfe['trade_receivable'],
                shortTermLoan=_bfe['short_term_loan'],
                netWorkingCapital=_bfe['networking_capital'],
                sellingGeneralAdministrativeExpenses=_bfe['admin_expenses'],
                createDate=create_date,
                updateDate=update_date
            )   # 전전기 재무 정보

        del df
        del _ths
        del _frm
        del _bfe
        return thstrm_company_finance, frmtrm_company_finance, bfefrmtrm_company_finance

    def preprocess(self, df: pd.DataFrame) -> Optional[List[NewCompanyFinancePydantic]]:
        """OpenDartReader를 이용해 수집한 기업 재무 정보를 DB에 저장하기 위해 전처리하는 함수
        Args:
            df (pd.DataFrame): 재무 정보 데이터프레임
        Returns:
            Optional[List[NewCompanyFinancePydantic]]: 전처리된 기업 재무 정보
        """
        results = []
        try:
            company_id = df['company_id'][0]    # company_id는 모두 동일
            biz_num, corporation_num, illu_id = self._cal_ids(company_id)   # 사업자등록번호, 법인등록번호, 일루넥스 ID
            create_date, update_date = get_current_date(), get_current_date()   # 생성일, 수정일
            years = df['bsns_year'].unique().tolist()   # 연도
            fs_divs = df['fs_div'].unique().tolist()    # 재무제표구분
            for year in years:
                for fs in fs_divs:
                    parsed_data = self._parse_company_finance(
                        df, company_id, biz_num, corporation_num, illu_id, year, fs, create_date, update_date
                        )
                    for data in parsed_data:
                        if data:
                            results.append(data)
            del parsed_data
            del df
            return results
        except Exception as e:
            err_msg = traceback.format_exc()
            self._logger.error(err_msg)
            self._logger.error(f"Error: {e}")
            return None
