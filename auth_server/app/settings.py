import os

from dotenv import load_dotenv

env_file = '.env'
load_dotenv(env_file)

SECRET_KEY = os.getenv('SECRET_KEY')
ALGORITHM = os.getenv('ALGORITHM')

USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')
