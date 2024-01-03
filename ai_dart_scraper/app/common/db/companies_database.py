import traceback
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd

from app.config.settings import COMPANIES_DB_URL
from app.models_init import NewCompanyInfo, NewScrapCompanyDartInfo, CodeClass
from app.common.log.log_config import setup_logger
from app.config.settings import FILE_PATHS
from app.common.core.utils import get_current_datetime, make_dir


class CompaniesDatabase:
    def __init__(self) -> None:
        self.engine = create_engine(COMPANIES_DB_URL, pool_recycle=3600, pool_size=20, max_overflow=0)
        self.SessionLocal = sessionmaker(bind=self.engine)
        
        file_path = FILE_PATHS['log'] + 'database'
        make_dir(file_path)
        file_path += f'/companies_{get_current_datetime()}.log'
        self.logger = setup_logger(
            "companies_database",
            file_path
        )
        self.company_id_dict = {}   # {corporation_num: company_id, ...}
        self._transform_list_to_dict()
        self.company_ids_from_newscrapcompanydartinfo = self._query_company_ids_from_newscrapcompanydartinfo()  # [company_id, ...]

    @contextmanager
    def get_session(self):
        """세션 컨텍스트 매니저"""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def get_ksic(self) -> pd.DataFrame:
        """CodeClass 테이블에서 code_class_id가 0042인 데이터만 조회하는 함수
        Returns:
            pd.DataFrame: 조회한 데이터프레임 -> [code_value, code_desc]
        """
        with self.get_session() as session:
            try:
                existing_data = session.query(CodeClass.code_value, CodeClass.code_desc).filter(CodeClass.code_class_id == '0042').all()
                return pd.DataFrame(existing_data, columns=['code_value', 'code_desc'])
            except SQLAlchemyError as e:
                err_msg = traceback.format_exc()
                self.logger.error(f"Error: {e}\n{err_msg}")
                return pd.DataFrame()

    def query_companies(self, company_id: int = None, biz_num: str = None, corporation_num: str = None) -> dict:
        """기업 정보를 조회하는 함수
        Args:
            company_id (str): 기업 ID
            biz_num (str): 사업자등록번호
            corporation_num (str): 법인등록번호
        Returns:
            NewCompanyInfo: 기업 정보
        """
        with self.get_session() as session:
            try:
                if company_id:
                    existing_data = session.query(NewCompanyInfo).filter(NewCompanyInfo.id == company_id).first()
                elif biz_num:
                    existing_data = session.query(NewCompanyInfo).filter(NewCompanyInfo.biz_num == biz_num).first()
                elif corporation_num:
                    existing_data = session.query(NewCompanyInfo).filter(NewCompanyInfo.corporation_num == corporation_num).first()
                else:
                    return None
                return existing_data.to_dict()
            except SQLAlchemyError as e:
                err_msg = traceback.format_exc()
                self.logger.error(f"Error: {e}\n{err_msg}")
                return None

    def _query_ids_and_corpnums_from_newscrapcompanydartinfo(self) -> list:
        with self.get_session() as session:
            try:
                existing_data = session.query(NewScrapCompanyDartInfo.company_id, NewScrapCompanyDartInfo.corporation_num).all()
                return existing_data    # [(company_id, corporation_num), ...]
            except SQLAlchemyError as e:
                err_msg = traceback.format_exc()
                self.logger.error(f"Error: {e}\n{err_msg}")

    def _transform_list_to_dict(self) -> None:
        existing_data = self._query_ids_and_corpnums_from_newscrapcompanydartinfo()
        if existing_data:
            for data in existing_data:
                self.company_id_dict[data[1]] = data[0]

    def _query_company_ids_from_newscrapcompanydartinfo(self) -> list:
        with self.get_session() as session:
            try:
                existing_data = session.query(NewScrapCompanyDartInfo.company_id).all()
                return [data[0] for data in existing_data]    # [company_id, ...]
            except SQLAlchemyError as e:
                err_msg = traceback.format_exc()
                self.logger.error(f"Error: {e}\n{err_msg}")
                return []

