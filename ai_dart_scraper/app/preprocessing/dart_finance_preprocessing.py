from typing import List, Dict

from app.common.log.log_config import setup_logger
from app.config.settings import FILE_PATHS
from app.common.core.utils import get_current_datetime, make_dir


class DartFinancePreprocessing:
    def __init__(self):
        file_path = FILE_PATHS["log"] + f'preprocessing'
        make_dir(file_path)
        file_path += f'/dart_finance_preprocessing_{get_current_datetime()}.log'
        self._logger = setup_logger(
            "dart_finance_preprocessing",
            file_path
        )

    def preprocess(self, data) -> List[Dict]:
        pass
