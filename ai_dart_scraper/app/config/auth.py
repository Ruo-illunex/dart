from fastapi import Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
import jwt
from jwt import PyJWTError

from app.config.settings import SECRET_KEY, ALGORITHM

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


def verify_token(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except PyJWTError:
        raise HTTPException(
            status_code=401,
            detail="Invalid authentication credentials"
        )
