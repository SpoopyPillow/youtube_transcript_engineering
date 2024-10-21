import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from sqlalchemy import MetaData, Table
from sqlalchemy.dialects.postgresql import insert


def load_table(connection, table_name, data):
    if len(data) == 0:
        return
    
    video_dict = data.to_dict(orient="records")

    metadata = MetaData()
    metadata.create_all(connection)
    metadata.reflect(connection)
    table = Table(table_name, metadata)

    upsert_query = insert(table).values(video_dict).on_conflict_do_nothing()

    connection.execute(upsert_query)
