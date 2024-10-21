import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from youtube_transcript_engineering.etl_tools.etl import *
from youtube_transcript_engineering.db_tools import *
from youtube_transcript_engineering.search_tools import *
from youtube_transcript_engineering.__init__ import engine

CHANNEL_ID = channel_name_to_id(input())

with engine.connect() as connection:
    create_tables(connection)

# with engine.connect() as connection:
#     clear_tables(connection)

with engine.connect() as connection:
    end_pages = channels_etl(connection, CHANNEL_ID, 5, verbose=True)
print(end_pages)
