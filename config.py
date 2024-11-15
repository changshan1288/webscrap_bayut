import os
import sys
from dotenv import load_dotenv

if getattr(sys, 'frozen', False):
    # If running as a frozen executable (PyInstaller), use sys._MEIPASS
    UTILS_DIR = os.path.dirname(os.path.realpath(sys.executable))
else:
    # If running as a regular script, use the current script's path
    UTILS_DIR = os.path.dirname(os.path.realpath(__file__))

load_dotenv(os.path.join(UTILS_DIR, '.env'))

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DB = os.getenv("MYSQL_DB")
MYSQL_DB_OTHERS = os.getenv("MYSQL_DB_OTHER")

MYSQL_PORT = os.getenv("MYSQL_PORT")

TABLE_NAME = os.getenv("TABLE_NAME")

STATUS_TABLE_NAME = os.getenv("STATUS_TABLE_NAME")

TRAND_TABLE_NAME = os.getenv("TRAND_TABLE_NAME")

count = 0

created = ""

main_db = ""

status_db = ""