import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from sqlalchemy import text


def create_tables(connection):
    connection.execute(
        text(
            "CREATE TABLE IF NOT EXISTS videos ("
            + "video_id CHAR(11),"
            + "title VARCHAR(255),"
            + "publish_date TIMESTAMP WITH TIME ZONE,"
            + "channel_id VARCHAR(255),"
            + "view_count INTEGER,"
            + "like_count INTEGER,"
            + "PRIMARY KEY (video_id));"
        )
    )
    connection.execute(
        text(
            "CREATE TABLE IF NOT EXISTS videos_tags ("
            + "video_id CHAR(11),"
            + "tag VARCHAR(255),"
            + "PRIMARY KEY (video_id, tag));"
        )
    )
    connection.execute(
        text(
            "CREATE TABLE IF NOT EXISTS playlists ("
            + "playlist_id VARCHAR(255),"
            + "playlist_name VARCHAR(255),"
            + "PRIMARY KEY (playlist_id));"
        )
    )
    connection.execute(
        text(
            "CREATE TABLE IF NOT EXISTS playlists_videos ("
            + "playlist_id VARCHAR(255),"
            + "video_id CHAR(11),"
            + "PRIMARY KEY (playlist_id, video_id));"
        )
    )
    connection.execute(
        text(
            "CREATE TABLE IF NOT EXISTS channels ("
            + "channel_id VARCHAR(255),"
            + "channel_name VARCHAR(255),"
            + "upload_id VARCHAR(255),"
            + "subscriber_count INTEGER,"
            + "PRIMARY KEY (channel_id));"
        )
    )
    connection.execute(
        text(
            "CREATE TABLE IF NOT EXISTS transcripts ("
            + "video_id CHAR(11),"
            + "position INTEGER,"
            + "group_position INTEGER,"
            + "word VARCHAR(255),"
            + "start FLOAT,"
            + "duration FLOAT,"
            + "PRIMARY KEY (video_id, position));"
        )
    )


def clear_tables(connection):
    connection.execute(text("DELETE FROM videos;"))
    connection.execute(text("DELETE FROM videos_tags;"))
    connection.execute(text("DELETE FROM playlists;"))
    connection.execute(text("DELETE FROM playlists_videos;"))
    connection.execute(text("DELETE FROM channels;"))
    connection.execute(text("DELETE FROM transcripts;"))
