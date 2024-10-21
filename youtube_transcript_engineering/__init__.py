import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("YOUTUBE_API_KEY")
SQL_USERNAME = os.getenv("SQL_USERNAME")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_HOST = os.getenv("SQL_HOST")

from googleapiclient.discovery import build

youtube = build("youtube", "v3", developerKey=API_KEY)

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

url = URL.create(
    drivername="postgresql",
    username=SQL_USERNAME,
    password=SQL_PASSWORD,
    host=SQL_HOST,
    database="youtube_transcript",
)

engine = create_engine(url, isolation_level="AUTOCOMMIT")
