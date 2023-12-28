import traceback
from typing import List, Optional

import pandas as pd

from app.common.log.log_config import setup_logger
from app.config.settings import FILE_PATHS
from app.common.core.utils import get_current_datetime, make_dir
from app.models_init import NewCompanyFinancePydantic
from app.common.db.companies_database import CompaniesDatabase


class DartFinancePreprocessing:
    def __init__(self):
        file_path = FILE_PATHS["log"] + f'preprocessing'
        make_dir(file_path)
        file_path += f'/dart_finance_preprocessing_{get_current_datetime()}.log'
        self._logger = setup_logger(
            "dart_finance_preprocessing",
            file_path
        )
        self._companies_db = CompaniesDatabase()

    def _get_ids(self, company_id: str) -> tuple:
        """기업 ID로 사업자등록번호와 법인등록번호를 조회하는 함수
        Args:
            company_id (str): 기업 ID
        Returns:
            tuple: (사업자등록번호, 법인등록번호)
        """
        data = self._companies_db.query_companies(company_id=company_id)
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

    def _search_values(self, df: pd.DataFrame, account_nm: str, sj_div: str, alt_account_nm_ls: list = []) -> tuple:
        """항목명으로 해당 항목의 값들을 찾는 함수
        Args:
            df (pd.DataFrame): 재무 정보 데이터프레임
            account_nm (str): 항목명
            sj_div (str): 재무제표구분 -> BS: 재무상태표, CIS: 손익계산서, CF: 현금흐름표
        Returns:
            tuple: (당기, 전기, 전전기)
        """
        thstrm_amount, frmtrm_amount, bfefrmtrm_amount = None, None, None
        cond1 = (df.account_nm == account_nm)
        cond2 = (df['thstrm_nm'].str.contains(r'제\s+\d+\s+기$'))
        cond3 = (df.sj_div == sj_div)
        try:
            thstrm_amount = df[cond1 & cond2 & cond3].thstrm_amount.values[0]
            frmtrm_amount = df[cond1 & cond2 & cond3].frmtrm_amount.values[0]
            bfefrmtrm_amount = df[cond1 & cond2 & cond3].bfefrmtrm_amount.values[0]
        # 항목명이 없을 경우
        except IndexError:
            # 대체 항목명이 있을 경우
            if alt_account_nm_ls:
                for alt_account_nm in alt_account_nm_ls:
                    thstrm_amount, frmtrm_amount, bfefrmtrm_amount = self._search_values(df, alt_account_nm, sj_div)
                    # 값이 있을 경우, 해당 값 반환
                    if thstrm_amount != '':
                        return thstrm_amount, frmtrm_amount, bfefrmtrm_amount
            err_msg = traceback.format_exc()
            self._logger.error(err_msg)
        except Exception as e:
            err_msg = traceback.format_exc()
            self._logger.error(err_msg)
            self._logger.error(f"Error: {e}")
        finally:
            # 값이 없을 경우 빈 문자열로 대체
            thstrm_amount = self._preprocess_values(thstrm_amount)
            frmtrm_amount = self._preprocess_values(frmtrm_amount)
            bfefrmtrm_amount = self._preprocess_values(bfefrmtrm_amount)
            return thstrm_amount, frmtrm_amount, bfefrmtrm_amount

    def _get_financial_dept_ratio(self, capital_total: str, dept_total: str) -> str:
        """부채비율을 계산하는 함수
        Args:
            capital_total (str): 자본총계
            dept_total (str): 부채총계
        Returns:
            str: 부채비율
        """
        assert capital_total != '', '자본총계가 없습니다.'
        assert dept_total != '', '부채총계가 없습니다.'
        try:
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
        return financial_dept_ratio

    def preprocess(self, df: pd.DataFrame) -> Optional[List[NewCompanyFinancePydantic]]:
        """OpenDartReader를 이용해 수집한 기업 재무 정보를 DB에 저장하기 위해 전처리하는 함수
        Args:
            data (List[CollectDartFinancePydantic]): OpenDartReader를 이용해 수집한 기업 재무 정보
        Returns:
            NewCompanyFinancePydantic: DB에 저장하기 위해 전처리한 기업 재무 정보
        """
        results = []
        try:
            company_id = df['company_id'][0]
            biz_num, corporation_num, illu_id = self._get_ids(company_id)
            # data를 연도별, fs_div별로 분리
            years = df['bsns_year'].unique().tolist()
            fs_divs = df['fs_div'].unique().tolist()
            for year in years:
                for fs in fs_divs:
                    _df = df.loc[(df['bsns_year'] == year) & (df['fs_div'] == fs)]
                    financial_decide_code=_df.fs_div.values[0]
                    financial_decide_desc=_df.fs_nm.values[0]
                    thstrm_year, frmtrm_year, bfefrmtrm_year = year, str(int(year)-1), str(int(year)-2)
                    thstrm_sales, frmtrm_sales, bfefrmtrm_sales = self._search_values(_df, '매출액', sj_div='CIS', alt_account_nm_ls=['수익(매출액)'])
                    thstrm_sales_cost, frmtrm_sales_cost, bfefrmtrm_sales_cost = self._search_values(_df, '매출원가', sj_div='CIS')
                    thstrm_operating_profit, frmtrm_operating_profit, bfefrmtrm_operating_profit = self._search_values(_df, '영업이익', sj_div='CIS', alt_account_nm_ls=['영업이익(손실)'])
                    thstrm_net_profit, frmtrm_net_profit, bfefrmtrm_net_profit = self._search_values(_df, '당기순이익(손실)', sj_div='CF')
                    thstrm_capital_amount, frmtrm_capital_amount, bfefrmtrm_capital_amount = self._search_values(_df, '자본금', sj_div='BS')
                    thstrm_capital_total, frmtrm_capital_total, bfefrmtrm_capital_total = self._search_values(_df, '자본총계', sj_div='BS')
                    thstrm_dept_total, frmtrm_dept_total, bfefrmtrm_dept_total = self._search_values(_df, '부채총계', sj_div='BS')
                    thstrm_assets_total, frmtrm_assets_total, bfefrmtrm_assets_total = self._search_values(_df, '자산총계', sj_div='BS')
                    thstrm_comprehensive_income, frmtrm_comprehensive_income, bfefrmtrm_comprehensive_income = self._search_values(_df, '총포괄손익', sj_div='CIS')
                    thstrm_tangible_asset, frmtrm_tangible_asset, bfefrmtrm_tangible_asset = self._search_values(_df, '유형자산', sj_div='BS')
                    thstrm_none_tangible_asset, frmtrm_none_tangible_asset, bfefrmtrm_none_tangible_asset = self._search_values(_df, '무형자산', sj_div='BS', alt_account_nm_ls=['영업권 이외의 무형자산'])
                    thstrm_current_asset, frmtrm_current_asset, bfefrmtrm_current_asset = self._search_values(_df, '유동자산', sj_div='BS')
                    thstrm_none_current_asset, frmtrm_none_current_asset, bfefrmtrm_none_current_asset = self._search_values(_df, '비유동자산', sj_div='BS')
                    thstrm_current_liabilities, frmtrm_current_liabilities, bfefrmtrm_current_liabilities = self._search_values(_df, '유동부채', sj_div='BS')
                    thstrm_inventories_asset, frmtrm_inventories_asset, bfefrmtrm_inventories_asset = self._search_values(_df, '재고자산', sj_div='BS')
                    thstrm_accounts_payable, frmtrm_accounts_payable, bfefrmtrm_accounts_payable = self._search_values(_df, '매입채무', sj_div='BS', alt_account_nm_ls=['매입채무 및 기타유동채무'])
                    thstrm_trade_receivable, frmtrm_trade_receivable, bfefrmtrm_trade_receivable = self._search_values(_df, '매출채권', sj_div='BS', alt_account_nm_ls=['매출채권 및 기타유동채권'])
                    thstrm_short_term_loan, frmtrm_short_term_loan, bfefrmtrm_short_term_loan = self._search_values(_df, '단기차입금', sj_div='BS')
                    thstrm_selling_general_administrative_expenses, frmtrm_selling_general_administrative_expenses, bfefrmtrm_selling_general_administrative_expenses = self._search_values(_df, '판매비와관리비', sj_div='CIS')
                    thstrm_financial_debt_ratio, frmtrm_financial_debt_ratio, bfefrmtrm_financial_debt_ratio = self._get_financial_dept_ratio(thstrm_capital_total, thstrm_dept_total), self._get_financial_dept_ratio(frmtrm_capital_total, frmtrm_dept_total), self._get_financial_dept_ratio(bfefrmtrm_capital_total, bfefrmtrm_dept_total)
                    
                    thstrm_company_finance = NewCompanyFinancePydantic(
                        company_id = company_id,
                        biz_num = biz_num,
                        corporation_num = corporation_num,
                        illu_id = illu_id,
                        acct_dt=thstrm_year,
                        financial_decide_code=financial_decide_code,
                        financial_decide_desc=financial_decide_desc,
                        sales = thstrm_sales,
                        sales_cost = thstrm_sales_cost,
                        operating_profit = thstrm_operating_profit,
                        net_profit = thstrm_net_profit,
                        capital_amount = thstrm_capital_amount,
                        capital_total = thstrm_capital_total,
                        debt_total = thstrm_dept_total,
                        asset_total = thstrm_assets_total,
                        comprehensive_income = thstrm_comprehensive_income,
                        financial_debt_ratio = thstrm_financial_debt_ratio,
                        tangible_asset = thstrm_tangible_asset,
                        non_tangible_asset = thstrm_none_tangible_asset,
                        current_asset = thstrm_current_asset,
                        non_current_asset = thstrm_none_current_asset,
                        current_liabilities = thstrm_current_liabilities,
                        inventories_asset = thstrm_inventories_asset,
                        accounts_payable = thstrm_accounts_payable,
                        trade_receivable = thstrm_trade_receivable,
                        short_term_loan = thstrm_short_term_loan,
                        selling_general_administrative_expenses = thstrm_selling_general_administrative_expenses
                    )
                    
                    frmtrm_company_finance = NewCompanyFinancePydantic(
                        company_id = company_id,
                        biz_num = biz_num,
                        corporation_num = corporation_num,
                        illu_id = illu_id,
                        acct_dt=frmtrm_year,
                        financial_decide_code=financial_decide_code,
                        financial_decide_desc=financial_decide_desc,
                        sales = frmtrm_sales,
                        sales_cost = frmtrm_sales_cost,
                        operating_profit = frmtrm_operating_profit,
                        net_profit = frmtrm_net_profit,
                        capital_amount = frmtrm_capital_amount,
                        capital_total = frmtrm_capital_total,
                        debt_total = frmtrm_dept_total,
                        asset_total = frmtrm_assets_total,
                        comprehensive_income = frmtrm_comprehensive_income,
                        financial_debt_ratio = frmtrm_financial_debt_ratio,
                        tangible_asset = frmtrm_tangible_asset,
                        non_tangible_asset = frmtrm_none_tangible_asset,
                        current_asset = frmtrm_current_asset,
                        non_current_asset = frmtrm_none_current_asset,
                        current_liabilities = frmtrm_current_liabilities,
                        inventories_asset = frmtrm_inventories_asset,
                        accounts_payable = frmtrm_accounts_payable,
                        trade_receivable = frmtrm_trade_receivable,
                        short_term_loan = frmtrm_short_term_loan,
                        selling_general_administrative_expenses = frmtrm_selling_general_administrative_expenses
                    )
                    
                    bfefrmtrm_company_finance = NewCompanyFinancePydantic(
                        company_id = company_id,
                        biz_num = biz_num,
                        corporation_num = corporation_num,
                        illu_id = illu_id,
                        acct_dt=bfefrmtrm_year,
                        financial_decide_code=financial_decide_code,
                        financial_decide_desc=financial_decide_desc,
                        sales = bfefrmtrm_sales,
                        sales_cost = bfefrmtrm_sales_cost,
                        operating_profit = bfefrmtrm_operating_profit,
                        net_profit = bfefrmtrm_net_profit,
                        capital_amount = bfefrmtrm_capital_amount,
                        capital_total = bfefrmtrm_capital_total,
                        debt_total = bfefrmtrm_dept_total,
                        asset_total = bfefrmtrm_assets_total,
                        comprehensive_income = bfefrmtrm_comprehensive_income,
                        financial_debt_ratio = bfefrmtrm_financial_debt_ratio,
                        tangible_asset = bfefrmtrm_tangible_asset,
                        non_tangible_asset = bfefrmtrm_none_tangible_asset,
                        current_asset = bfefrmtrm_current_asset,
                        non_current_asset = bfefrmtrm_none_current_asset,
                        current_liabilities = bfefrmtrm_current_liabilities,
                        inventories_asset = bfefrmtrm_inventories_asset,
                        accounts_payable = bfefrmtrm_accounts_payable,
                        trade_receivable = bfefrmtrm_trade_receivable,
                        short_term_loan = bfefrmtrm_short_term_loan,
                        selling_general_administrative_expenses = bfefrmtrm_selling_general_administrative_expenses
                    )
                    
                    results.append(thstrm_company_finance)
                    results.append(frmtrm_company_finance)
                    results.append(bfefrmtrm_company_finance)
            return results
        except Exception as e:
            err_msg = traceback.format_exc()
            self._logger.error(err_msg)
            self._logger.error(f"Error: {e}")
            return None
