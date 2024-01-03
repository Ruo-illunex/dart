import os
import datetime
import io
import zipfile

import requests
import xmltodict
import pandas as pd

from app.config.settings import DART_API_KEY


def make_dir(path):
    """디렉토리 생성 함수

    Args:
        path (str): 생성할 디렉토리 경로
    """
    if not os.path.exists(path):
        os.makedirs(path)
        msg = f'Directory created: {path}'
        print(msg)
        return msg
    else:
        msg = f'Directory already exists: {path}'
        print(msg)
        return msg


def get_current_datetime():
    """현재 시간을 반환하는 함수

    Returns:
        str: 현재 시간 문자열
    """
    return datetime.datetime.now().strftime('%Y%m%d%H%M%S')


def get_current_date():
    """현재 날짜를 반환하는 함수

    Returns:
        str: 현재 날짜 문자열
    """
    return datetime.datetime.now().strftime('%Y-%m-%d')


def get_corp_codes() -> dict:
    url = 'https://opendart.fss.or.kr/api/corpCode.xml'
    params = {'crtfc_key': DART_API_KEY}
    
    resp = requests.get(url, params=params)
    if resp.status_code != 200:
        raise Exception(f"Error: {resp.status_code} {resp.reason}")
    else:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            # ZIP 파일 내용 추출
            with zf.open('CORPCODE.xml') as file:
                data = file.read()
                data = data.decode('utf-8')
    return pd.DataFrame(xmltodict.parse(data)['result']['list'])
