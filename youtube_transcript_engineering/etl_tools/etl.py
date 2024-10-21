import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from etl_tools.extract import *
from etl_tools.load import *
from etl_tools.transform import *


def videos_etl(connection, video_ids, verbose=False):
    if isinstance(video_ids, str):
        video_ids = [video_ids]

    success = [True] * len(video_ids)
    visited_channels = set()
    for counter, video_id in enumerate(video_ids):
        if verbose:
            print(
                "Video #" + str(counter + 1) + " of " + str(len(video_ids)) + " extracting...",
                end="",
            )

        try:
            videos = extract_videos(video_id)
            video_data = transform_videos(videos)
            video_tags_data = transform_videos_tags(videos)

            transcripts = extract_transcripts(video_id)
            transcript_data = transform_transcripts(transcripts)
        except:
            print(" Failed")
            success[counter] = False
            continue

        load_table(connection, "videos", video_data)
        load_table(connection, "videos_tags", video_tags_data)
        load_table(connection, "transcripts", transcript_data)

        channel_id = video_data["channel_id"][0]
        channels = extract_channels(channel_id)

        channel_data = transform_channels(channels)
        playlist_videos_data = pd.DataFrame(
            {"playlist_id": channel_data["upload_id"], "video_id": video_id}
        )

        load_table(connection, "playlists_videos", playlist_videos_data)

        if channel_id not in visited_channels:
            extracted = extract_playlists(channel_data["upload_id"][0], 0)[1]
            playlist_data = transform_playlists(extracted)[0]

            load_table(connection, "channels", channel_data)
            load_table(connection, "playlists", playlist_data)

            visited_channels.add(channel_id)

        if verbose:
            print()

    return success


def playlists_etl(connection, playlist_ids, max_results=50, start_page=None, verbose=False):
    if isinstance(playlist_ids, str):
        playlist_ids = [playlist_ids]
    if not isinstance(max_results, list):
        max_results = [max_results] * len(playlist_ids)

    end_pages = {}
    for counter, playlist_id in enumerate(playlist_ids):
        if verbose:
            print(
                "Playlist #" + str(counter + 1) + " of " + str(len(playlist_ids)) + " extracting..."
            )

        end_page, extracted = extract_playlists(playlist_id, max_results[counter], start_page)
        playlist_data, playlist_videos_data = transform_playlists(extracted)

        has_transcript = videos_etl(connection, playlist_videos_data["video_id"], verbose=verbose)
        playlist_videos_data = playlist_videos_data[has_transcript]

        load_table(connection, "playlists", playlist_data)
        load_table(connection, "playlists_videos", playlist_videos_data)

        end_pages[playlist_id] = end_page

    return end_pages


def channels_etl(connection, channel_ids, max_results=50, start_page=None, verbose=False):
    if isinstance(channel_ids, str):
        channel_ids = [channel_ids]
    if not isinstance(max_results, list):
        max_results = [max_results] * len(channel_ids)

    end_pages = {}
    for counter, channel_id in enumerate(channel_ids):
        if verbose:
            print(
                "Channel #" + str(counter + 1) + " of " + str(len(channel_ids)) + " extracting..."
            )

        channels = extract_channels(channel_id)
        channel_data = transform_channels(channels)
        load_table(connection, "channels", channel_data)

        playlist_end_pages = playlists_etl(
            connection,
            channel_data["upload_id"],
            max_results=max_results[counter],
            start_page=start_page,
            verbose=verbose,
        )

        end_pages[channel_id] = next(iter(playlist_end_pages.values()))

    return end_pages
