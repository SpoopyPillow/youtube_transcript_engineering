import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from youtube_transcript_api import YouTubeTranscriptApi
from etl_tools import youtube


def extract_videos(video_id):
    request = youtube.videos().list(part="snippet,statistics", id=video_id)

    return (video_id, request.execute())


def extract_playlists(playlist_id, max_results=50, start_page=None):
    playlist_name = (
        youtube.playlists()
        .list(part="snippet", id=playlist_id)
        .execute()["items"][0]["snippet"]["title"]
    )

    playlist_videos = []
    current_left = max_results if max_results != -1 else 50
    next_page = start_page
    end_page = None

    while current_left > 0:
        request = youtube.playlistItems().list(
            part="snippet, contentDetails",
            playlistId=playlist_id,
            maxResults=current_left,
            pageToken=next_page,
        )

        response = request.execute()

        playlist_videos += response["items"]

        end_page = next_page
        next_page = response.get("nextPageToken")
        if max_results != -1:
            current_left -= 50

        if next_page is None:
            end_page = -1
            break

    end_page = end_page if end_page is not None else 0
    return end_page, (playlist_id, playlist_name, playlist_videos)


def extract_channels(channel_id):
    request = youtube.channels().list(part="snippet,contentDetails,statistics", id=channel_id)

    return (channel_id, request.execute())


def extract_transcripts(video_id):
    transcript = YouTubeTranscriptApi.get_transcript(video_id, languages=["en"])

    return (video_id, transcript)
