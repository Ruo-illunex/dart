import traceback

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.config.settings import COMPANIES_DB_URL
from app.models_init import NewCompanyInfo, NewScrapCompanyDartInfo, CodeClass
from app.common.log.log_config import setup_logger
from app.config.settings import FILE_PATHS
from app.common.core.utils import get_current_datetime, make_dir


class CompaniesDatabase:
    def __init__(self) -> None:
        self.engine = create_engine(COMPANIES_DB_URL, pool_recycle=3600, pool_size=20, max_overflow=0)
        self.SessionLocal = sessionmaker(bind=self.engine)
        file_path = f'{FILE_PATHS['log']}database/companies_{get_current_datetime()}.log'
        make_dir(file_path)
        self.logger = setup_logger(
            "companies_database",
            file_path
        )
        self.company_id_dict = {}   # {corporation_num: company_id, ...}
        self._transform_list_to_dict()

    def _query_ids_and_corpnums_from_newscrapcompanydartinfo(self) -> list:
        session = self.SessionLocal()
        try:
            existing_data = session.query(NewScrapCompanyDartInfo.company_id, NewScrapCompanyDartInfo.corporation_num).all()
            return existing_data    # [(company_id, corporation_num), ...]
        except Exception as e:
            err_msg = traceback.format_exc()
            self.logger.error(f"Error: {e}\n{err_msg}")
        finally:
            session.close()

    def _transform_list_to_dict(self) -> None:
        existing_data = self._query_ids_and_corpnums_from_newscrapcompanydartinfo()
        for data in existing_data:
            self.company_id_dict[data[1]] = data[0]

