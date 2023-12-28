from datetime import datetime, date

from sqlalchemy import Column, String, Integer, TIMESTAMP, Date
from pydantic import BaseModel

from app.common.db.base import BaseCompanies


class NewCompanyFinance(BaseCompanies):
    """기업 재무 정보 모델 클래스"""
    __tablename__ = 'new_company_finance'

    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, nullable=True)
    biz_num = Column(String(10), nullable=True)
    corporation_num = Column(String(13), nullable=True)
    illu_id = Column(String(15), nullable=True)
    acct_dt = Column(String(8), default='', nullable=True, comment='재무일자')
    financial_decide_code = Column(String(100), default='', nullable=True, comment='재무제표구분코드')
    financial_decide_desc = Column(String(255), default='', nullable=True, comment='재무제표구분명')
    sales = Column(String(50), default='', nullable=True, comment='기업 매출금액')
    sales_cost = Column(String(50), default='', nullable=True, comment='매출원가')
    operating_profit = Column(String(50), default='', nullable=True, comment='기업 영업이익')
    net_profit = Column(String(50), default='', nullable=True, comment='기업 당기순이익')
    capital_amount = Column(String(50), default='', nullable=True, comment='기업 자본금액')
    capital_total = Column(String(50), default='', nullable=True, comment='기업 총 자본금액')
    debt_total = Column(String(50), default='', nullable=True, comment='기업 총 부채금액')
    asset_total = Column(String(50), default='', nullable=True, comment='기업 총 자산금액')
    comprehensive_income = Column(String(50), default='', nullable=True, comment='포괄손익계산금액')
    financial_debt_ratio = Column(String(100), default='', nullable=True, comment='재무제표 부채비율')
    tangible_asset = Column(String(50), default='', nullable=True, comment='유형자산')
    non_tangible_asset = Column(String(50), default='', nullable=True, comment='비유형자산')
    current_asset = Column(String(50), default='', nullable=True, comment='유동자산')
    non_current_asset = Column(String(50), default='', nullable=True, comment='비유동자산')
    current_liabilities = Column(String(50), default='', nullable=True, comment='유동부채')
    net_worth = Column(String(50), default='', nullable=True, comment='자기자본')
    quick_asset = Column(String(50), default='', nullable=True, comment='당좌자산')
    inventories_asset = Column(String(50), default='', nullable=True, comment='재고자산')
    accounts_payable = Column(String(50), default='', nullable=True, comment='매입채무')
    trade_receivable = Column(String(50), default='', nullable=True, comment='매출채권')
    short_term_loan = Column(String(50), default='', nullable=True, comment='단기차입금')
    net_working_capital = Column(String(50), default='', nullable=True, comment='순운전자본')
    selling_general_administrative_expenses = Column(String(50), default='', nullable=True, comment='매출액 판매관리비')
    create_date = Column(Date, default=date.today)
    update_date = Column(Date, default=date.today, onupdate=date.today)
    finance_timestamp = Column(TIMESTAMP, default=datetime.utcnow, onupdate=datetime.utcnow)

    # 인코딩 설정
    __table_args__ = {
        'mysql_charset': 'utf8mb4',
        'mysql_collate': 'utf8mb4_unicode_ci'
    }



class NewCompanyFinancePydantic(BaseModel):
    """기업 재무 정보 Pydantic 모델"""
    # 모델 필드 정의
    company_id: int = None
    biz_num: str = None
    corporation_num: str = None
    illu_id: str = None
    acct_dt: str = None
    financial_decide_code: str = None
    financial_decide_desc: str = None
    sales: str = None
    sales_cost: str = None
    operating_profit: str = None
    net_profit: str = None
    capital_amount: str = None
    capital_total: str = None
    debt_total: str = None
    asset_total: str = None
    comprehensive_income: str = None
    financial_debt_ratio: str = None
    tangible_asset: str = None
    non_tangible_asset: str = None
    current_asset: str = None
    non_current_asset: str = None
    current_liabilities: str = None
    net_worth: str = None
    quick_asset: str = None
    inventories_asset: str = None
    accounts_payable: str = None
    trade_receivable: str = None
    short_term_loan: str = None
    # net_working_capital: str = None
    selling_general_administrative_expenses: str = None
    
    class Config:
        from_attributes = True
