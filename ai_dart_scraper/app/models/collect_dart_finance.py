from datetime import datetime
from typing import Optional

from sqlalchemy import Column, Integer, String, BigInteger, Date
from pydantic import BaseModel

from app.common.db.base import BaseCollections


class CollectDartFinance(BaseCollections):
    """DART에서 수집한 재무 정보 모델 클래스"""

    __tablename__ = 'collect_dart_finance'

    # 테이블 컬럼 정의
    id = Column(BigInteger, primary_key=True, autoincrement=True, comment='고유번호')
    company_id = Column(Integer, nullable=True, comment='companies.new_company_info.id')
    rcept_no = Column(String(14), comment='접수번호')
    reprt_code = Column(String(5), comment='보고서 코드')
    bsns_year = Column(Integer, comment='사업 연도')
    corp_code = Column(String(8), comment='고유번호')
    fs_div = Column(String(3), comment='개별/연결구분')
    fs_nm = Column(String(100), comment='개별/연결명')
    sj_div = Column(String(10), comment='재무제표 구분')
    sj_nm = Column(String(100), comment='재무제표 명')
    account_id = Column(String(20), comment='계정ID')
    account_nm = Column(String(255), comment='계정명')
    account_detail = Column(String(255), nullable=True, comment='계정상세')
    thstrm_nm = Column(String(45), nullable=True, comment='당기명')
    thstrm_amount = Column(BigInteger, nullable=True, comment='당기금액')
    thstrm_add_amount = Column(BigInteger, nullable=True, comment='당기누적금액')
    frmtrm_nm = Column(String(45), nullable=True, comment='전기명')
    frmtrm_amount = Column(BigInteger, nullable=True, comment='전기금액')
    frmtrm_q_nm = Column(String(45), nullable=True, comment='전기분기명')
    frmtrm_q_amount = Column(BigInteger, nullable=True, comment='전기분기금액')
    frmtrm_add_amount = Column(BigInteger, nullable=True, comment='전기누적금액')
    bfefrmtrm_nm = Column(String(45), nullable=True, comment='전전기명')
    bfefrmtrm_amount = Column(BigInteger, nullable=True, comment='전전기금액')
    ord = Column(Integer, comment='계정정렬순서')
    currency = Column(String(10), comment='통화 단위')

    # 인코딩 설정
    __table_args__ = {
        'mysql_charset': 'utf8mb4',
        'mysql_collate': 'utf8mb4_unicode_ci'
    }


class CollectDartFinancePydantic(BaseModel):
    """DART에서 수집한 재무 정보 Pydantic 모델"""

    # 모델 필드 정의
    company_id: Optional[int] = None
    rcept_no: Optional[str] = None
    reprt_code: Optional[str] = None
    bsns_year: Optional[int] = None
    corp_code: Optional[str] = None
    fs_div: Optional[str] = None
    fs_nm: Optional[str] = None
    sj_div: Optional[str] = None
    sj_nm: Optional[str] = None
    account_id: Optional[str] = None
    account_nm: Optional[str] = None
    account_detail: Optional[str] = None
    thstrm_nm: Optional[str] = None
    thstrm_amount: Optional[int] = None
    thstrm_add_amount: Optional[int] = None
    frmtrm_nm: Optional[str] = None
    frmtrm_amount: Optional[int] = None
    frmtrm_q_nm: Optional[str] = None
    frmtrm_q_amount: Optional[int] = None
    frmtrm_add_amount: Optional[int] = None
    bfefrmtrm_nm: Optional[str] = None
    bfefrmtrm_amount: Optional[int] = None
    ord: Optional[int] = None
    currency: Optional[str] = None

    class Config:
        from_attributes = True
