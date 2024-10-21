import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd


def transform_videos(video_response):
    video_id, response = video_response

    video_data = pd.DataFrame(
        [response["items"][0]["snippet"] | response["items"][0]["statistics"]]
    )
    video_data["video_id"] = video_id

    video_data = video_data[
        ["video_id", "title", "publishedAt", "channelId", "viewCount", "likeCount"]
    ]
    video_data["publishedAt"] = pd.to_datetime(video_data["publishedAt"]).dt.tz_convert("UTC")

    video_data.rename(
        columns={
            "publishedAt": "publish_date",
            "channelId": "channel_id",
            "viewCount": "view_count",
            "likeCount": "like_count",
        },
        inplace=True,
    )

    return video_data


def transform_videos_tags(video_response):
    video_id, response = video_response
    tags = response["items"][0]["snippet"]["tags"]

    video_tags_data = pd.DataFrame({"video_id": video_id, "tag": tags})

    video_tags_data["tag"] = (
        video_tags_data["tag"]
        .str.lower()
        .str.replace("\n", "")
        .replace(r"\(.*\)|\[.*\]|\{.*\}", "", regex=True)
        .replace("[^a-zA-Z0-9 ]", "", regex=True)
    )

    return video_tags_data


def transform_playlists(playlist_response):
    playlist_id, playlist_name, response = playlist_response
    video_ids = list(map(lambda x: x["snippet"]["resourceId"]["videoId"], response))

    playlist_data = pd.DataFrame([{"playlist_id": playlist_id, "playlist_name": playlist_name}])
    playlist_videos_data = pd.DataFrame({"playlist_id": playlist_id, "video_id": video_ids})

    return playlist_data, playlist_videos_data


def transform_channels(channel_response):
    channel_id, response = channel_response

    channel_name = response["items"][0]["snippet"]["title"]
    upload_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    subscribers = response["items"][0]["statistics"]["subscriberCount"]

    channel_data = pd.DataFrame(
        [
            {
                "channel_id": channel_id,
                "channel_name": channel_name,
                "upload_id": upload_id,
                "subscriber_count": subscribers,
            }
        ]
    )

    return channel_data


def transform_transcripts(video_transcript):
    video_id, transcript = video_transcript

    transcript_data = pd.DataFrame(transcript)

    transcript_data["video_id"] = video_id

    transcript_data["text"] = (
        transcript_data["text"]
        .str.lower()
        .str.replace("\n", " ")
        .replace(r"\(.*\)|\[.*\]|\{.*\}", "", regex=True)
        .replace("[^a-zA-Z0-9 ]", "", regex=True)
        .str.split()
    )

    transcript_data = transcript_data.explode("text").dropna()

    transcript_data = transcript_data.reset_index()
    transcript_data["group_position"] = (
        transcript_data.groupby(["index", "video_id"]).cumcount() + 1
    )
    transcript_data["index"] = transcript_data.groupby("video_id").cumcount() + 1

    transcript_data["duration"] = transcript_data["duration"].round(3)
    transcript_data["start"] = transcript_data["start"].round(3)

    transcript_data.rename(columns={"text": "word", "index": "position"}, inplace=True)

    transcript_data = transcript_data[
        ["video_id", "position", "group_position", "word", "start", "duration"]
    ]

    return transcript_data
