import traceback
from typing import List
from contextlib import contextmanager
from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd

from app.config.settings import COLLECTIONS_DB_URL
from app.common.log.log_config import setup_logger
from app.config.settings import FILE_PATHS
from app.common.core.utils import get_current_datetime, make_dir
from app.models_init import CollectDart, CollectDartPydantic, CollectDartFinance, CollectDartFinancePydantic
from app.common.db.companies_database import CompaniesDatabase


class CollectionsDatabase:
    def __init__(self) -> None:
        self.engine = create_engine(COLLECTIONS_DB_URL, pool_recycle=3600, pool_size=20, max_overflow=0)
        self.SessionLocal = sessionmaker(bind=self.engine)
        file_path = FILE_PATHS["log"] + f'database'
        make_dir(file_path)
        file_path += f'/collections_{get_current_datetime()}.log'
        self.logger = setup_logger(
            "collections_database",
            file_path
        )
        self.last_queried_id_collectdart = None
        self._companies_db = CompaniesDatabase()
        self._company_ids_from_newscrapcompanydartinfo = self._companies_db.company_ids_from_newscrapcompanydartinfo    # [company_id, ...]

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

    def get_companyids_and_corpcodes(self) -> list:
        """CollectDart 테이블에서 company_id와 corp_code를 조회하는 함수
        Returns:
            list: [(company_id, corp_code), ...]
        """
        with self.get_session() as session:
            try:
                # self._company_ids_from_newscrapcompanydartinfo 에 있는 company_id와 stock_code가 있는 데이터만 조회
                existing_data = session.query(CollectDart.company_id, CollectDart.corp_code).filter(
                    CollectDart.company_id.in_(self._company_ids_from_newscrapcompanydartinfo),
                    CollectDart.stock_code != ''
                ).all()
                # 순서를 역순으로 정렬
                existing_data.reverse() # <- 추후에는 제거
                return existing_data    # [(company_id, corp_code), ...]
            except SQLAlchemyError as e:
                err_msg = traceback.format_exc()
                self.logger.error(f"Error: {e}\n{err_msg}")

    def bulk_upsert_data_collectdart(self, data_list: List[CollectDartPydantic]) -> None:
        """데이터베이스에 데이터를 일괄 추가 또는 업데이트하는 함수
        Args:
            data_list (List[CollectDartPydantic]): 추가 또는 업데이트할 데이터 리스트
        """
        with self.get_session() as session:
            try:
                to_add = []  # 새로 추가할 데이터를 저장할 리스트
                for data in data_list:
                    existing_data = session.query(CollectDart).filter(CollectDart.corp_code == data.corp_code).first()
                    if existing_data:
                        # 변경된 데이터의 키를 가져오고 해당 필드만 업데이트
                        update_data = data.dict(exclude_unset=True)
                        for key, value in update_data.items():
                            setattr(existing_data, key, value)
                    else:
                        # 새 데이터 객체 생성 및 추가
                        new_data = CollectDart(**data.dict())
                        to_add.append(new_data)

                # 새로 추가할 데이터가 있으면 일괄 처리
                if to_add:
                    session.bulk_save_objects(to_add)

                session.commit()
            except SQLAlchemyError as e:
                session.rollback()
                err_msg = traceback.format_exc()
                self.logger.error(f"Error: {e}\n{err_msg}")

    def check_if_exists_collectdartfinance(self, corp_code: str, bsns_year: str, reprt_code: str, fs_div: str) -> bool:
        """데이터베이스에 데이터가 있는지 확인하는 함수
        Args:
            corp_code (str): 기업 코드
            bsns_year (str): 사업 연도
            reprt_code (str): 보고서 코드
            fs_div (str): 재무제표 구분
        Returns:
            bool: 데이터가 있으면 True, 없으면 False
        """
        with self.get_session() as session:
            try:
                existing_data = session.query(CollectDartFinance).filter(
                    CollectDartFinance.corp_code == corp_code,
                    CollectDartFinance.reprt_code == reprt_code,
                    CollectDartFinance.bsns_year == bsns_year,
                    CollectDartFinance.fs_div == fs_div
                ).first()
                if existing_data:
                    return True
                else:
                    return False
            except SQLAlchemyError as e:
                err_msg = traceback.format_exc()
                self.logger.error(f"Error: {e}\n{err_msg}")

    def bulk_insert_collectdartfinance(self, data_list: List[CollectDartFinancePydantic]) -> None:
        """데이터베이스에 데이터를 일괄 추가하는 함수
        Args:
            data_list (List[CollectDartFinancePydantic]): 추가할 데이터 리스트
        """
        with self.get_session() as session:
            try:
                if not data_list:
                    result_msg = "No data to insert"
                    return result_msg

                company_id = data_list[0].company_id
                insert_data = [data.dict() for data in data_list]
                insert_stmt = insert(CollectDartFinance).values(insert_data)
                session.execute(insert_stmt)
                session.commit()
                result_msg = f"Success: Inserted {len(data_list)} data for company_id {company_id}"
                return result_msg
            except SQLAlchemyError as e:
                session.rollback()
                err_msg = traceback.format_exc()
                self.logger.error(f"Error: {e}\n{err_msg}")
                return err_msg

    def query_collectdart(self, biz_num: str = None, corp_num: str = None, company_id: int = None) -> Optional[CollectDartPydantic]:
        """데이터베이스에서 데이터를 조회하는 함수"""
        with self.get_session() as session:
            try:
                assert biz_num or corp_num or company_id, "biz_num, corp_num, company_id 중 하나는 필수로 입력해야 합니다."
                if biz_num:
                    existing_data = session.query(CollectDart).filter(CollectDart.bizr_no == biz_num).first()
                elif corp_num:
                    existing_data = session.query(CollectDart).filter(CollectDart.jurir_no == corp_num).first()
                elif company_id:
                    existing_data = session.query(CollectDart).filter(CollectDart.company_id == int(company_id)).first()
                if existing_data:
                    # id, create_date, update_date를 제외한 모든 필드를 CollectDartPydantic 모델로 변환
                    return CollectDartPydantic.from_orm(existing_data)
            except AssertionError as e:
                err_msg = traceback.format_exc()
                self.logger.error(f"Error: {e}\n{err_msg}")
                return None
            except Exception as e:
                err_msg = traceback.format_exc()
                self.logger.error(f"Error: {e}\n{err_msg}")
                return None

    def query_collectdartfinance(self, biz_num: str = None, corp_num: str = None, company_id: int = None) -> pd.DataFrame:
        """데이터베이스에서 데이터를 조회하는 함수"""
        with self.get_session() as session:
            try:
                assert biz_num or corp_num or company_id, "biz_num, corp_num, company_id 중 하나는 필수로 입력해야 합니다."
                if biz_num:
                    company_id = self._companies_db.query_companies(biz_num=biz_num).get('id')
                elif corp_num:
                    company_id = self._companies_db.query_companies(corporation_num=corp_num).get('id')
                if company_id:
                    existing_data = session.query(CollectDartFinance).filter(CollectDartFinance.company_id == company_id).all()
                    if existing_data:
                        df = pd.DataFrame([data.to_dict() for data in existing_data])
                        return df
                    else:
                        raise Exception(f"해당 기업의 재무 정보가 없습니다. biz_num: {biz_num}, corp_num: {corp_num}, company_id: {company_id}")
            except AssertionError as e:
                err_msg = traceback.format_exc()
                self.logger.error(f"Error: {e}\n{err_msg}")
                return pd.DataFrame()
            except Exception as e:
                err_msg = traceback.format_exc()
                self.logger.error(f"Error: {e}\n{err_msg}")
                return pd.DataFrame()
