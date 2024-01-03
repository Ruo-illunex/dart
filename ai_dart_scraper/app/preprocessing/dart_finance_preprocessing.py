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
        file_path = FILE_PATHS["log"] + f'preprocessing'
        make_dir(file_path)
        file_path += f'/dart_finance_preprocessing_{get_current_datetime()}.log'
        self._logger = setup_logger(
            "dart_finance_preprocessing",
            file_path
        )

    def _get_ids(self, company_id: str) -> tuple:
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
                        thstrm_amount, frmtrm_amount, bfefrmtrm_amount = self._search_values(df, account_nm=alt_account_nm, sj_div=sj_div)
                        if thstrm_amount != '':
                            return thstrm_amount, frmtrm_amount, bfefrmtrm_amount
                if alt_account_id_ls:
                    for alt_account_id in alt_account_id_ls:
                        thstrm_amount, frmtrm_amount, bfefrmtrm_amount = self._search_values(df, sj_div=sj_div, account_id=alt_account_id)
                        if thstrm_amount != '':
                            return thstrm_amount, frmtrm_amount, bfefrmtrm_amount
                if alt_sj_div_ls:
                    for alt_sj_div in alt_sj_div_ls:
                        thstrm_amount, frmtrm_amount, bfefrmtrm_amount = self._search_values(df, sj_div=alt_sj_div, account_nm=account_nm, account_id=account_id)
                        if thstrm_amount != '':
                            return thstrm_amount, frmtrm_amount, bfefrmtrm_amount
                return '', '', ''
        except Exception as e:
            err_msg = traceback.format_exc()
            self._logger.error(err_msg)
            self._logger.error(f"Error: {e}")
            return '', '', ''

    def _get_financial_dept_ratio(self, capital_total: str, dept_total: str) -> str:
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

    def _get_net_worth(self, asset_total: str, dept_total: str) -> str:
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

    def _get_quick_asset(self, current_asset: str, inventories_asset: str) -> str:
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

    def _get_net_working_capital(self, current_asset: str, current_liabilities: str) -> str:
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

    def preprocess(self, df: pd.DataFrame) -> Optional[List[NewCompanyFinancePydantic]]:
        """OpenDartReader를 이용해 수집한 기업 재무 정보를 DB에 저장하기 위해 전처리하는 함수
        Args:
            df (pd.DataFrame): 재무 정보 데이터프레임
        Returns:
            Optional[List[NewCompanyFinancePydantic]]: 전처리된 기업 재무 정보
        """
        results = []
        try:
            company_id = df['company_id'][0]
            biz_num, corporation_num, illu_id = self._get_ids(company_id)
            create_date, update_date = get_current_date(), get_current_date()
            # data를 연도별, fs_div별로 분리
            years = df['bsns_year'].unique().tolist()
            fs_divs = df['fs_div'].unique().tolist()
            for year in years:
                for fs in fs_divs:
                    _df = df.loc[(df['bsns_year'] == year) & (df['fs_div'] == fs)]
                    financial_decide_code = _df.fs_div.values[0]
                    financial_decide_desc = _df.fs_nm.values[0]
                    thstrm_year, frmtrm_year, bfefrmtrm_year = year, str(int(year)-1), str(int(year)-2)
                    thstrm_sales, frmtrm_sales, bfefrmtrm_sales = self._search_values(_df, account_id='ifrs-full_Revenue', alt_account_nm_ls=['매출액'])
                    thstrm_sales_cost, frmtrm_sales_cost, bfefrmtrm_sales_cost = self._search_values(_df, account_id='ifrs-full_CostOfSales', alt_account_nm_ls=['매출원가'])
                    thstrm_operating_profit, frmtrm_operating_profit, bfefrmtrm_operating_profit = self._search_values(_df, account_id='dart_OperatingIncomeLoss', alt_account_nm_ls=['영업이익'])
                    thstrm_net_profit, frmtrm_net_profit, bfefrmtrm_net_profit = self._search_values(_df, sj_div='CIS', account_id='ifrs-full_ProfitLoss', alt_sj_div_ls=['IS'], alt_account_nm_ls=['당기순이익', '당기순이익(손실)'])
                    thstrm_capital_amount, frmtrm_capital_amount, bfefrmtrm_capital_amount = self._search_values(_df, account_nm='자본금', sj_div='BS')
                    thstrm_capital_total, frmtrm_capital_total, bfefrmtrm_capital_total = self._search_values(_df, account_nm='자본총계', sj_div='BS')
                    thstrm_dept_total, frmtrm_dept_total, bfefrmtrm_dept_total = self._search_values(_df, account_nm='부채총계', sj_div='BS')
                    thstrm_assets_total, frmtrm_assets_total, bfefrmtrm_assets_total = self._search_values(_df, account_nm='자산총계', sj_div='BS')
                    thstrm_comprehensive_income, frmtrm_comprehensive_income, bfefrmtrm_comprehensive_income = self._search_values(_df, sj_div='CIS', account_id='ifrs-full_ComprehensiveIncome', alt_account_id_ls=['ifrs_ComprehensiveIncome'], alt_account_nm_ls=['총포괄손익'])
                    thstrm_tangible_asset, frmtrm_tangible_asset, bfefrmtrm_tangible_asset = self._search_values(_df, account_id='ifrs-full_PropertyPlantAndEquipment', alt_account_id_ls=['ifrs_PropertyPlantAndEquipment'], alt_account_nm_ls=['유형자산'])
                    thstrm_none_tangible_asset, frmtrm_none_tangible_asset, bfefrmtrm_none_tangible_asset = self._search_values(_df, account_id='ifrs-full_IntangibleAssetsOtherThanGoodwill', alt_account_id_ls=['dart_OtherIntangibleAssetsGross', 'dart_GoodwillGross'], alt_account_nm_ls=['무형자산'])
                    thstrm_current_asset, frmtrm_current_asset, bfefrmtrm_current_asset = self._search_values(_df, account_id='ifrs-full_CurrentAssets', alt_account_id_ls=['ifrs_CurrentAssets'], alt_account_nm_ls=['유동자산'])
                    thstrm_none_current_asset, frmtrm_none_current_asset, bfefrmtrm_none_current_asset = self._search_values(_df, account_id='ifrs-full_NoncurrentAssets', alt_account_id_ls=['ifrs_NonCurrentAssets'], alt_account_nm_ls=['비유동자산'])
                    thstrm_current_liabilities, frmtrm_current_liabilities, bfefrmtrm_current_liabilities = self._search_values(_df, account_id='ifrs-full_CurrentLiabilities', alt_account_id_ls=['ifrs_CurrentLiabilities'], alt_account_nm_ls=['유동부채'])
                    thstrm_inventories_asset, frmtrm_inventories_asset, bfefrmtrm_inventories_asset = self._search_values(_df, account_id='ifrs-full_Inventories', alt_account_id_ls=['ifrs_Inventories'], alt_account_nm_ls=['재고자산'])
                    thstrm_accounts_payable, frmtrm_accounts_payable, bfefrmtrm_accounts_payable = self._search_values(_df, sj_div='BS', account_id='ifrs-full_TradeAndOtherCurrentPayables', alt_account_id_ls=['dart_ShortTermTradePayables'], alt_account_nm_ls=['매입채무', '단기매입채무'])
                    thstrm_trade_receivable, frmtrm_trade_receivable, bfefrmtrm_trade_receivable = self._search_values(_df, sj_div='BS', account_id='ifrs-full_TradeAndOtherCurrentReceivables', alt_account_id_ls=['dart_ShortTermTradeReceivable'], alt_account_nm_ls=['매출채권', '단기매출채권'])
                    thstrm_short_term_loan, frmtrm_short_term_loan, bfefrmtrm_short_term_loan = self._search_values(_df, sj_div='BS', account_id='ifrs-full_ShorttermBorrowings', alt_account_nm_ls=['단기차입금'])
                    thstrm_selling_general_administrative_expenses, frmtrm_selling_general_administrative_expenses, bfefrmtrm_selling_general_administrative_expenses = self._search_values(_df, account_id='dart_TotalSellingGeneralAdministrativeExpenses')
                    thstrm_financial_debt_ratio, frmtrm_financial_debt_ratio, bfefrmtrm_financial_debt_ratio = self._get_financial_dept_ratio(thstrm_capital_total, thstrm_dept_total), self._get_financial_dept_ratio(frmtrm_capital_total, frmtrm_dept_total), self._get_financial_dept_ratio(bfefrmtrm_capital_total, bfefrmtrm_dept_total)
                    thstrm_net_worth, frmtrm_net_worth, bfefrmtrm_net_worth = self._get_net_worth(thstrm_assets_total, thstrm_dept_total), self._get_net_worth(frmtrm_assets_total, frmtrm_dept_total), self._get_net_worth(bfefrmtrm_assets_total, bfefrmtrm_dept_total)
                    thstrm_quick_asset, frmtrm_quick_asset, bfefrmtrm_quick_asset = self._get_quick_asset(thstrm_current_asset, thstrm_inventories_asset), self._get_quick_asset(frmtrm_current_asset, frmtrm_inventories_asset), self._get_quick_asset(bfefrmtrm_current_asset, bfefrmtrm_inventories_asset)
                    thstrm_networking_capital, frmtrm_networking_capital, bfefrmtrm_networking_capital = self._get_net_working_capital(thstrm_current_asset, thstrm_current_liabilities), self._get_net_working_capital(frmtrm_current_asset, frmtrm_current_liabilities), self._get_net_working_capital(bfefrmtrm_current_asset, bfefrmtrm_current_liabilities)

                    thstrm_company_finance = NewCompanyFinancePydantic(
                        companyId=company_id,
                        bizNum=biz_num,
                        corporationNum=corporation_num,
                        illuId=illu_id,
                        acctDt=thstrm_year,
                        financialDecideCode=financial_decide_code,
                        financialDecideDesc=financial_decide_desc,
                        sales=thstrm_sales,
                        salesCost=thstrm_sales_cost,
                        operatingProfit=thstrm_operating_profit,
                        netProfit=thstrm_net_profit,
                        capitalAmount=thstrm_capital_amount,
                        capitalTotal=thstrm_capital_total,
                        debtTotal=thstrm_dept_total,
                        assetTotal=thstrm_assets_total,
                        comprehensiveIncome=thstrm_comprehensive_income,
                        financialDebtRatio=thstrm_financial_debt_ratio,
                        tangibleAsset=thstrm_tangible_asset,
                        nonTangibleAsset=thstrm_none_tangible_asset,
                        currentAsset=thstrm_current_asset,
                        nonCurrentAsset=thstrm_none_current_asset,
                        currentLiabilities=thstrm_current_liabilities,
                        netWorth=thstrm_net_worth,
                        quickAsset=thstrm_quick_asset,
                        inventoriesAsset=thstrm_inventories_asset,
                        accountsPayable=thstrm_accounts_payable,
                        tradeReceivable=thstrm_trade_receivable,
                        shortTermLoan=thstrm_short_term_loan,
                        netWorkingCapital=thstrm_networking_capital,
                        sellingGeneralAdministrativeExpenses=thstrm_selling_general_administrative_expenses,
                        createDate=create_date,
                        updateDate=update_date
                    )

                    frmtrm_company_finance = NewCompanyFinancePydantic(
                        companyId=company_id,
                        bizNum=biz_num,
                        corporationNum=corporation_num,
                        illuId=illu_id,
                        acctDt=frmtrm_year,
                        financialDecideCode=financial_decide_code,
                        financialDecideDesc=financial_decide_desc,
                        sales=frmtrm_sales,
                        salesCost=frmtrm_sales_cost,
                        operatingProfit=frmtrm_operating_profit,
                        netProfit=frmtrm_net_profit,
                        capitalAmount=frmtrm_capital_amount,
                        capitalTotal=frmtrm_capital_total,
                        debtTotal=frmtrm_dept_total,
                        assetTotal=frmtrm_assets_total,
                        comprehensiveIncome=frmtrm_comprehensive_income,
                        financialDebtRatio=frmtrm_financial_debt_ratio,
                        tangibleAsset=frmtrm_tangible_asset,
                        nonTangibleAsset=frmtrm_none_tangible_asset,
                        currentAsset=frmtrm_current_asset,
                        nonCurrentAsset=frmtrm_none_current_asset,
                        currentLiabilities=frmtrm_current_liabilities,
                        netWorth=frmtrm_net_worth,
                        quickAsset=frmtrm_quick_asset,
                        inventoriesAsset=frmtrm_inventories_asset,
                        accountsPayable=frmtrm_accounts_payable,
                        tradeReceivable=frmtrm_trade_receivable,
                        shortTermLoan=frmtrm_short_term_loan,
                        netWorkingCapital=frmtrm_networking_capital,
                        sellingGeneralAdministrativeExpenses=frmtrm_selling_general_administrative_expenses,
                        createDate=create_date,
                        updateDate=update_date
                    )

                    bfefrmtrm_company_finance = NewCompanyFinancePydantic(
                        companyId=company_id,
                        bizNum=biz_num,
                        corporationNum=corporation_num,
                        illuId=illu_id,
                        acctDt=bfefrmtrm_year,
                        financialDecideCode=financial_decide_code,
                        financialDecideDesc=financial_decide_desc,
                        sales=bfefrmtrm_sales,
                        salesCost=bfefrmtrm_sales_cost,
                        operatingProfit=bfefrmtrm_operating_profit,
                        netProfit=bfefrmtrm_net_profit,
                        capitalAmount=bfefrmtrm_capital_amount,
                        capitalTotal=bfefrmtrm_capital_total,
                        debtTotal=bfefrmtrm_dept_total,
                        assetTotal=bfefrmtrm_assets_total,
                        comprehensiveIncome=bfefrmtrm_comprehensive_income,
                        financialDebtRatio=bfefrmtrm_financial_debt_ratio,
                        tangibleAsset=bfefrmtrm_tangible_asset,
                        nonTangibleAsset=bfefrmtrm_none_tangible_asset,
                        currentAsset=bfefrmtrm_current_asset,
                        nonCurrentAsset=bfefrmtrm_none_current_asset,
                        currentLiabilities=bfefrmtrm_current_liabilities,
                        netWorth=bfefrmtrm_net_worth,
                        quickAsset=bfefrmtrm_quick_asset,
                        inventoriesAsset=bfefrmtrm_inventories_asset,
                        accountsPayable=bfefrmtrm_accounts_payable,
                        tradeReceivable=bfefrmtrm_trade_receivable,
                        shortTermLoan=bfefrmtrm_short_term_loan,
                        netWorkingCapital=bfefrmtrm_networking_capital,
                        sellingGeneralAdministrativeExpenses=bfefrmtrm_selling_general_administrative_expenses,
                        createDate=create_date,
                        updateDate=update_date
                    )

                    results.append(thstrm_company_finance)
                    results.append(frmtrm_company_finance)
                    results.append(bfefrmtrm_company_finance)
            # 메모리 해제
            del df
            del _df
            return results
        except Exception as e:
            err_msg = traceback.format_exc()
            self._logger.error(err_msg)
            self._logger.error(f"Error: {e}")
            return None
