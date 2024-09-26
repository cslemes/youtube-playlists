import os

import isodate
import pandas as pd
import requests

apikey = os.getenv("API_KEY")

URL = "https://www.googleapis.com/youtube/v3/playlists"
PLAYLIST_ITEMS_URL = "https://www.googleapis.com/youtube/v3/playlistItems"
VIDEOS_URL = "https://www.googleapis.com/youtube/v3/videos"


def get_playlist_video_ids(api_key, playlist_id):
    video_ids = []
    params = {
        "part": "contentDetails",
        "playlistId": playlist_id,
        "maxResults": 50,  # Máximo permitido por requisição
        "key": api_key,
    }
    next_page_token = None

    while True:
        if next_page_token:
            params["pageToken"] = next_page_token
        response = requests.get(PLAYLIST_ITEMS_URL, params=params)
        data = response.json()
        video_ids += [item["contentDetails"]["videoId"] for item in data["items"]]

        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            break

    return video_ids


def get_videos_duration(api_key, video_ids):
    total_duration = isodate.Duration()
    # Requisição em lotes de até 50 IDs
    for i in range(0, len(video_ids), 50):
        ids_batch = ",".join(video_ids[i : i + 50])
        params = {"part": "contentDetails", "id": ids_batch, "key": api_key}
        response = requests.get(VIDEOS_URL, params=params)
        data = response.json()

        # Somar a duração de cada vídeo
        for video in data["items"]:
            duration = isodate.parse_duration(video["contentDetails"]["duration"])
            total_duration += duration

    return total_duration


def main(API_KEY, CHANNEL_ID):
    params = {
        "part": "snippet",
        "channelId": CHANNEL_ID,
        "forUsername": "@teomewhy",
        "maxResults": 25,
        "key": API_KEY,
    }

    response = requests.get(URL, params=params)

    if response.status_code == 200:
        playlists = response.json()

        playlist_data = []

        for playlist in playlists["items"]:
            video_playlist_id = get_playlist_video_ids(API_KEY, playlist["id"])
            total_duration = get_videos_duration(API_KEY, video_playlist_id)
            print(f"Título da Playlist: {playlist['snippet']['title']}")
            print(f"Duração: {total_duration}")
            print("-" * 40)

            playlist_data.append(
                {
                    "Título da Playlist": playlist["snippet"]["title"],
                    "Duração": str(
                        total_duration
                    ),  # Armazena a duração em formato legível
                }
            )

        df = pd.DataFrame(playlist_data)

        df.to_parquet("playlists_duracao.parquet")

    else:
        print(f"Erro: {response.status_code}, {response.text}")


if __name__ == "__main__":
    API_KEY = apikey  # api youtube data api v3
    CHANNEL_ID = "UC-Xa9J9-B4jBOoBNIHkMMKA"  # id do @teomewhy
    main(API_KEY, CHANNEL_ID)
