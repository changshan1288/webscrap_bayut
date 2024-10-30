import os

from dotenv import load_dotenv

UTILS_DIR = os.path.dirname(os.path.realpath(__file__))

load_dotenv(os.path.join(UTILS_DIR, '.env'))

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DB = os.getenv("MYSQL_DB")
MYSQL_PORT = os.getenv("MYSQL_PORT")

TABLE_NAME = os.getenv("TABLE_NAME")