from typing import List, Optional
import traceback

from fastapi import APIRouter, HTTPException, status, Depends
from pydantic import BaseModel

from app.database_init import collections_db
from app.models_init import NewCompanyFinancePydantic
from app.common.core.utils import get_current_datetime, make_dir
from app.config.settings import FILE_PATHS
from app.common.log.log_config import setup_logger
from app.preprocessing.dart_finance_preprocessing import DartFinancePreprocessing
from app.config.auth import verify_token


router = APIRouter()
file_path = FILE_PATHS["log"] + 'api'
make_dir(file_path)
file_path += f'/dart_finance_routers_{get_current_datetime()}.log'
logger = setup_logger(
    "dart_finance_routers",
    file_path
)

dart_finance_preprocessing = DartFinancePreprocessing()


class NewCompanyFinanceResponse(BaseModel):
    """기업 재무정보 응답 모델 클래스"""
    newCompanyFinance: List[NewCompanyFinancePydantic]


def get_company_info(bizNum: str = None, corpNum: str = None, companyId: str = None) -> Optional[List[NewCompanyFinancePydantic]]:
    """사업자등록번호로 기업 정보를 조회하는 함수
    Args:
        bizNum (str): 사업자등록번호
        db (Session): DB 세션
    Returns:
        NewCompanyInfoPydantic: 기업 정보
    """
    try:
        if bizNum:
            df = collections_db.query_collectdartfinance(biz_num=bizNum)
        elif corpNum:
            df = collections_db.query_collectdartfinance(corp_num=corpNum)
        elif companyId:
            df = collections_db.query_collectdartfinance(company_id=companyId)
        if df.empty:
            return None
        processed = dart_finance_preprocessing.preprocess(df)
        return processed
    except Exception as e:
        err_msg = traceback.format_exc()
        logger.error(err_msg)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/dart/finance/business/{bizNum}", response_model=NewCompanyFinanceResponse)
async def get_company_finance_info(bizNum: str, token: str = Depends(verify_token)):
    """사업자등록번호로 기업 재무정보를 조회하는 함수
    Args:
        bizNum (str): 사업자등록번호
    Returns:
        NewCompanyFinanceResponse: 기업 재무정보
    """
    try:
        data = get_company_info(bizNum=bizNum)
        if not data:
            return NewCompanyFinanceResponse(newCompanyFinance=[])
        return NewCompanyFinanceResponse(newCompanyFinance=data)
    except Exception as e:
        err_msg = traceback.format_exc()
        logger.error(err_msg)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/dart/finance/corporation/{corpNum}", response_model=NewCompanyFinanceResponse)
async def get_company_finance_info(corpNum: str, token: str = Depends(verify_token)):
    """법인등록번호로 기업 재무정보를 조회하는 함수
    Args:
        corpNum (str): 법인등록번호
    Returns:
        NewCompanyFinanceResponse: 기업 재무정보
    """
    try:
        data = get_company_info(corpNum=corpNum)
        if not data:
            return NewCompanyFinanceResponse(newCompanyFinance=[])
        return NewCompanyFinanceResponse(newCompanyFinance=data)
    except Exception as e:
        err_msg = traceback.format_exc()
        logger.error(err_msg)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/dart/finance/company/{companyId}", response_model=NewCompanyFinanceResponse)
async def get_company_finance_info(companyId: int, token: str = Depends(verify_token)):
    """기업 ID로 기업 재무정보를 조회하는 함수
    Args:
        companyId (int): 기업 ID
    Returns:
        NewCompanyFinanceResponse: 기업 재무정보
    """
    try:
        data = get_company_info(companyId=companyId)
        if not data:
            return NewCompanyFinanceResponse(newCompanyFinance=[])
        return NewCompanyFinanceResponse(newCompanyFinance=data)
    except Exception as e:
        err_msg = traceback.format_exc()
        logger.error(err_msg)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
