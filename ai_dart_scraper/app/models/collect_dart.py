from datetime import date

from sqlalchemy import Column, Integer, String, Date
from pydantic import BaseModel

from app.common.db.base import BaseCollections


class CollectDart(BaseCollections):
    """DART에서 수집한 기업 정보 모델 클래스"""

    __tablename__ = 'collect_dart'

    # 테이블 컬럼 정의
    id = Column(Integer, primary_key=True, autoincrement=True, comment='고유번호')
    company_id = Column(Integer, nullable=True, comment='companies.new_company_info.id')
    corp_code = Column(String(100), comment='법인 코드')
    corp_name = Column(String(100), comment='법인 이름')
    corp_name_eng = Column(String(100), comment='법인 영문 이름')
    stock_name = Column(String(100), comment='주식 이름')
    stock_code = Column(String(100), comment='주식 코드')
    ceo_nm = Column(String(100), comment='CEO 이름')
    corp_cls = Column(String(10), comment='법인 분류')
    jurir_no = Column(String(50), comment='법률 등록 번호')
    bizr_no = Column(String(50), comment='사업자 등록 번호')
    adres = Column(String(200), comment='주소')
    hm_url = Column(String(200), comment='홈페이지 URL')
    ir_url = Column(String(200), comment='IR 홈페이지 URL')
    phn_no = Column(String(50), comment='전화 번호')
    fax_no = Column(String(50), comment='팩스 번호')
    induty_code = Column(String(50), comment='산업 코드')
    est_dt = Column(String(50), comment='설립 날짜')
    acc_mt = Column(String(10), comment='회계 월')
    created_at = Column(Date, default=date.today, comment='생성 날짜')
    updated_at = Column(Date, onupdate=date.today, comment='수정 날짜')

    # 인코딩 설정
    __table_args__ = {
        'mysql_charset': 'utf8mb4',
        'mysql_collate': 'utf8mb4_unicode_ci'
    }


class CollectDartPydantic(BaseModel):
    """DART에서 수집한 기업 정보 Pydantic 모델"""

    # 모델 필드 정의
    company_id: int
    corp_code: str
    corp_name: str
    corp_name_eng: str
    stock_name: str
    stock_code: str
    ceo_nm: str
    corp_cls: str
    jurir_no: str
    bizr_no: str
    adres: str
    hm_url: str
    ir_url: str
    phn_no: str
    fax_no: str
    induty_code: str
    est_dt: str
    acc_mt: str
    created_at: date
    updated_at: date

    class Config:
        from_attributes = True
