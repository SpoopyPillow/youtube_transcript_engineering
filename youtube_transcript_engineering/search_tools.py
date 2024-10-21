import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from youtube_transcript_engineering import youtube

def channel_name_to_id(channel_name):
    request = youtube.channels().list(part="id", forUsername=channel_name)
    response = request.execute()
    
    return response["items"][0]["id"]
