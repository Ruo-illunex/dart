from datetime import datetime, timedelta
from typing import Optional
from passlib.context import CryptContext

from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
import jwt

from app.settings import SECRET_KEY, ALGORITHM, USERNAME, PASSWORD

ACCESS_TOKEN_EXPIRE_MINUTES = 60*24*7

# 비밀번호 해싱을 위한 설정
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# 관리자 자격 증명 설정
admin_credentials = {
    USERNAME: pwd_context.hash(PASSWORD)
}

app = FastAPI()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def verify_token(token: str, credentials_exception):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except jwt.PyJWTError:
        raise credentials_exception


# 사용자 인증 함수
def authenticate_user(username: str, password: str):
    hashed_password = admin_credentials.get(username)
    if not hashed_password:
        return False
    if not pwd_context.verify(password, hashed_password):
        return False
    return True


@app.post("/token")
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    user_authenticated = authenticate_user(form_data.username, form_data.password)
    if not user_authenticated:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": form_data.username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}


@app.get("/users/me")
async def read_users_me(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    return verify_token(token, credentials_exception)
