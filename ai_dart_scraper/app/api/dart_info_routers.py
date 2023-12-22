from typing import List, Optional
import traceback

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from app.common.db.collections_database import CollectionsDatabase
from app.common.db.companies_database import CompaniesDatabase
from app.models_init import NewCompanyInfoPydantic
from app.common.core.utils import get_current_datetime, make_dir
from app.config.settings import FILE_PATHS
from app.common.log.log_config import setup_logger
from app.preprocessing.dart_info_preprocessing import DartInfoPreprocessing


router = APIRouter()
file_path = FILE_PATHS["log"] + f'api'
make_dir(file_path)
file_path += f'/dart_info_routers_{get_current_datetime()}.log'
logger = setup_logger(
    "dart_info_routers",
    file_path
)
collections_db = CollectionsDatabase()
companies_db = CompaniesDatabase()

dart_info_preprocessing = DartInfoPreprocessing()


class NewCompanyInfoResponse(BaseModel):
    """기업 정보 응답 모델 클래스"""
    newCompanyInfo: List[NewCompanyInfoPydantic]

def get_company_info(bizNum: str = None, corpNum: str = None, companyId: str = None) -> Optional[NewCompanyInfoPydantic]:
    """사업자등록번호로 기업 정보를 조회하는 함수
    Args:
        bizNum (str): 사업자등록번호
        db (Session): DB 세션
    Returns:
        NewCompanyInfoPydantic: 기업 정보
    """
    try:
        if bizNum:
            data = collections_db.query_collectdart(biz_num=bizNum)
        elif corpNum:
            data = collections_db.query_collectdart(corp_code=corpNum)
        elif companyId:
            data = collections_db.query_collectdart(company_id=companyId)
        if not data:
            return None
        processed = dart_info_preprocessing.preprocess(data)
        return processed
    except Exception as e:
        err_msg = traceback.format_exc()
        logger.error(err_msg)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/dart/info/business/{bizNum}", response_model=NewCompanyInfoResponse)
def get_company_info_by_biznum_endpoint(bizNum: str):
    """사업자등록번호로 기업 정보를 조회하는 API
    Args:
        bizNum (str): 사업자등록번호
    Returns:
        List[NewCompanyInfo]: 기업 정보
    """
    if not bizNum:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="사업자등록번호를 입력해주세요.")
    try:
        data = get_company_info(bizNum)
        if data:
            return NewCompanyInfoResponse(newCompanyInfo=[data])
        else:
            return NewCompanyInfoResponse(newCompanyInfo=[])
    except Exception as e:
        err_msg = traceback.format_exc()
        logger.error(err_msg)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/dart/info/corporation/{corpNum}", response_model=NewCompanyInfoResponse)
def get_company_info_by_corpnum_endpoint(corpNum: str):
    """법인등록번호로 기업 정보를 조회하는 API
    Args:
        corpNum (str): 법인등록번호
    Returns:
        List[NewCompanyInfo]: 기업 정보
    """
    if not corpNum:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="법인등록번호를 입력해주세요.")
    try:
        data = get_company_info(corpNum)
        if data:
            return NewCompanyInfoResponse(newCompanyInfo=[data])
        else:
            return NewCompanyInfoResponse(newCompanyInfo=[])
    except Exception as e:
        err_msg = traceback.format_exc()
        logger.error(err_msg)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/dart/info/company/{companyId}", response_model=NewCompanyInfoResponse)
def get_company_info_by_companyid_endpoint(companyId: str):
    """기업 ID로 기업 정보를 조회하는 API
    Args:
        companyId (str): 기업 ID
    Returns:
        List[NewCompanyInfo]: 기업 정보
    """
    if not companyId:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="기업 ID를 입력해주세요.")
    try:
        data = get_company_info(companyId)
        if data:
            return NewCompanyInfoResponse(newCompanyInfo=[data])
        else:
            return NewCompanyInfoResponse(newCompanyInfo=[])
    except Exception as e:
        err_msg = traceback.format_exc()
        logger.error(err_msg)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
