from sqlalchemy import Column, String
from pydantic import BaseModel

from app.common.db.base import BaseCompanies


class CodeClass(BaseCompanies):
    """코드 분류 모델 클래스"""

    __tablename__ = 'code_class'

    # 테이블 컬럼 정의
    code_class_id = Column(String(255), primary_key=True)
    code_class_name = Column(String(255))
    code_value = Column(String(255))
    code_desc = Column(String(255))
    std_dt = Column(String(255))
    id = Column(String(255))

    # 인코딩 설정
    __table_args__ = {
        'mysql_charset': 'utf8mb4',
        'mysql_collate': 'utf8mb4_unicode_ci'
    }


class CodeClassPydantic(BaseModel):
    """코드 분류 Pydantic 모델"""

    # 모델 필드 정의
    code_class_id: str
    code_class_name: str
    code_value: str
    code_desc: str
    std_dt: str
    id: str

    class Config:
        from_attributes = True
