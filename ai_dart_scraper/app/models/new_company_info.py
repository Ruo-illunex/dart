from datetime import date
from typing import Optional

from sqlalchemy import Column, String, Integer, Text, Date, Boolean, CHAR, inspect
from pydantic import BaseModel

from app.common.db.base import BaseCompanies


class NewCompanyInfo(BaseCompanies):
    """기업 정보 모델 클래스"""

    __tablename__ = 'new_company_info'

    # 테이블 컬럼 정의
    id = Column(Integer, primary_key=True, autoincrement=True)
    biz_num = Column(String(13), comment='사업자등록번호')
    corporation_num = Column(String(13), comment='법인등록번호')
    company_name = Column(String(255), comment='기업명')
    real_company_name = Column(String(255), comment='순수 기업명')
    company_state = Column(String(255), comment='법인상태')
    representation_name = Column(String(60), comment='대표자명')
    company_type = Column(String(255), comment='기업유형')
    company_size = Column(String(255), comment='기업규모')
    employee_count = Column(String(10), comment='직원수')
    establishment_date = Column(String(20), comment='설립일')
    acct_month = Column(String(50), comment='재무일자')
    business_condition_code = Column(String(1), default='', comment='산업코드 업태 알파벳')
    business_condition_desc = Column(String(255), default='', comment='산업코드 업태명')
    business_category_code = Column(String(10), comment='산업코드')
    business_category_desc = Column(String(255), comment='산업코드명')
    homepage = Column(String(255), comment='홈페이지 주소')
    tel = Column(String(50), comment='전화번호')
    email = Column(String(255), default='', comment='대표 이메일')
    fax = Column(String(20), comment='팩스')
    address = Column(String(255), comment='주소')
    zip_code = Column(String(5), comment='우편번호')
    sales = Column(String(255), default='', comment='매출(천원)')
    sales_year = Column(String(255), default='', comment='매출년도')
    major_product = Column(String(255), comment='대표제품')
    head_office = Column(CHAR(1), default='1', comment='본사여부')
    category = Column(String(255), default='', comment='카테고리')
    keyword = Column(String(500), default='', comment='키워드')
    visible = Column(Boolean, default=True, comment='공개유무(1:공개, 0:비공개)')
    description = Column(Text, comment='소개')
    origin_id = Column(Integer, comment='원본 ID 구분')
    listing_market_id = Column(String(3), comment='상장시장구분코드')
    listing_market_desc = Column(String(10), comment='상장코드명')
    country = Column(String(255), comment='기업국가')
    logo_url = Column(String(500), comment='로고 URL')
    illu_id = Column(String(13))
    is_koscom_scrap_success = Column(Boolean, default=False, comment='코스콤 수집 성공여부')
    create_date = Column(Date, default=date.today, comment='생성일')
    update_date = Column(Date, default=date.today, onupdate=date.today, comment='수정일')

    # 인코딩 설정
    __table_args__ = {
        'mysql_charset': 'utf8mb4',
        'mysql_collate': 'utf8mb4_unicode_ci'
    }
    
    def to_dict(self):
        """모델 인스턴스를 딕셔너리로 변환하는 메서드"""
        return {c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs}


class NewCompanyInfoPydantic(BaseModel):
    """기업 정보 Pydantic 모델"""

    # 모델 필드 정의
    id: int
    bizNum: Optional[str] = None
    corporationNum: Optional[str] = None
    companyName: Optional[str] = None
    realCompanyName: Optional[str] = None
    companyState: Optional[str] = None
    representationName: Optional[str] = None
    companyType: Optional[str] = None
    companySize: Optional[str] = None
    employeeCount: Optional[str] = None
    establishmentDate: Optional[str] = None
    acctMonth: Optional[str] = None
    businessConditionCode: Optional[str] = None
    businessConditionDesc: Optional[str] = None
    businessCategoryCode: Optional[str] = None
    businessCategoryDesc: Optional[str] = None
    homepage: Optional[str] = None
    tel: Optional[str] = None
    email: Optional[str] = None
    fax: Optional[str] = None
    address: Optional[str] = None
    zipCode: Optional[str] = None
    sales: Optional[str] = None
    salesYear: Optional[str] = None
    majorProduct: Optional[str] = None
    headOffice: Optional[str] = None
    category: Optional[str] = None
    keyword: Optional[str] = None
    visible: Optional[bool] = None
    description: Optional[str] = None
    origin_id: Optional[int] = None
    listingMarketId: Optional[str] = None
    listingMarketDesc: Optional[str] = None
    country: Optional[str] = None
    logoUrl: Optional[str] = None
    illuId: Optional[str] = None
    isKoscomScrapSuccess: Optional[bool] = None
    createDate: date = None
    updateDate: date = None

    class Config:
        from_attributes = True
