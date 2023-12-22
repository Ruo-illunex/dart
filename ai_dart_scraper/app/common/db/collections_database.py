import traceback
from typing import List
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd

from app.config.settings import COLLECTIONS_DB_URL
from app.common.log.log_config import setup_logger
from app.config.settings import FILE_PATHS
from app.common.core.utils import get_current_datetime, make_dir
from app.models_init import CollectDart, CollectDartPydantic, CollectDartFinance, CollectDartFinancePydantic, CodeClass
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

    def get_ksic(self) -> pd.DataFrame:
        """CodeClass 테이블에서 code_class_id가 0042인 데이터만 조회하는 함수
        Returns:
            pd.DataFrame: 조회한 데이터프레임
        """
        with self.get_session() as session:
            try:
                existing_data = session.query(CodeClass.code_value, CodeClass.code_desc).filter(CodeClass.code_class_id == '0042').all()
                df = pd.DataFrame(existing_data, columns=['code_value', 'code_desc'])
                return df
            except SQLAlchemyError as e:
                err_msg = traceback.format_exc()
                self.logger.error(f"Error: {e}\n{err_msg}")

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

    def bulk_upsert_data_collectdartfinance(self, data_list: List[CollectDartFinancePydantic]) -> None:
        """데이터베이스에 데이터를 일괄 추가 또는 업데이트하는 함수
        Args:
            data_list (List[CollectDartFinancePydantic]): 추가 또는 업데이트할 데이터 리스트
        """
        with self.get_session() as session:
            to_add = []
            for data in data_list:
                existing_data = session.query(CollectDartFinance).filter(
                    CollectDartFinance.company_id == data.company_id,
                    CollectDartFinance.corp_code == data.corp_code,
                    CollectDartFinance.bsns_year == data.bsns_year,
                    CollectDartFinance.reprt_code == data.reprt_code,
                    CollectDartFinance.fs_div == data.fs_div,
                    CollectDartFinance.sj_div == data.sj_div,
                    CollectDartFinance.sj_nm == data.sj_nm,
                    CollectDartFinance.account_id == data.account_id,
                    CollectDartFinance.account_nm == data.account_nm,
                    CollectDartFinance.account_detail == data.account_detail
                ).first()
                if existing_data:
                    # 변경된 데이터의 키를 가져오고 해당 필드만 업데이트
                    update_data = data.dict(exclude_unset=True)
                    for key, value in update_data.items():
                        setattr(existing_data, key, value)
                else:
                    # 새 데이터 객체 생성 및 추가
                    new_data = CollectDartFinance(**data.dict())
                    to_add.append(new_data)

            # 새로 추가할 데이터가 있으면 일괄 처리
            if to_add:
                session.bulk_save_objects(to_add)
                
            session.commit()

    def query_collectdart(self, batchsize: int = 1000) -> list:
        """데이터베이스에서 데이터를 조회하는 함수
        Args:
            batchsize (int, optional): 조회할 데이터의 개수. Defaults to 1000.
        Returns:
            list: 조회한 데이터 리스트
        """
        with self.get_session() as session:
            try:
                # 마지막으로 조회한 id가 있으면 해당 id보다 큰 id를 가진 데이터를 조회
                if self.last_queried_id_collectdart:
                    existing_data = session.query(CollectDart).filter(
                        CollectDart.id > self.last_queried_id_collectdart
                    ).limit(batchsize).all()
                    info_msg = f"Last queried id: {self.last_queried_id_collectdart}"
                    self.logger.info(info_msg)
                    print(info_msg)
                # 마지막으로 조회한 id가 없으면 처음부터 batchsize만큼 데이터를 조회
                else:
                    existing_data = session.query(CollectDart).limit(batchsize).all()
                    info_msg = "No last queried id"
                    self.logger.info(info_msg)
                    print(info_msg)

                # 마지막으로 조회한 id를 업데이트
                if existing_data:
                    self.last_queried_id_collectdart = existing_data[-1].id
                return existing_data
            except Exception as e:
                err_msg = traceback.format_exc()
                self.logger.error(f"Error: {e}\n{err_msg}")
                return []
