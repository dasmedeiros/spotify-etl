# Import necessary libraries
from urllib.parse import parse_qs, urlparse
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from spotify_token import SpotifyTokenManager, SpotifyConfig, HttpClient

# Class for ETL (Extract, Transform, Load) operations on Spotify data
class SpotifyETL:
    def __init__(self, token_manager, recent_tracks_url, tracks_details_url, tracks_features_url):
        self.token_manager = token_manager
        self.http_client = HttpClient()
        self.recent_tracks_url = recent_tracks_url
        self.tracks_details_url = tracks_details_url
        self.tracks_features_url = tracks_features_url

    # Method to retrieve Spotify API headers with a valid access token
    def get_spotify_headers(self):
        access_token = self.token_manager.get_access_token()

        if access_token is None:
            print("Failed to obtain a valid access token.")
            return None

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "Bearer {token}".format(token=access_token)
        }

        return headers

    # Method to extract recently played tracks
    def extract_recently_played_tracks(self):
        headers = self.get_spotify_headers()
        
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

        # Initialize an empty list to store all tracks
        all_tracks = []

        while True:
            # Download tracks for the current page
            url = f"{self.recent_tracks_url}?after={yesterday_unix_timestamp}"
            r = self.http_client.get(url, headers=headers)

            if r.status_code != 200:
                print("Failed to fetch data from Spotify API. Status code:", r.status_code)
                return None  # Return None to indicate an error.

            try:
                data = r.json()
                tracks = data["items"]

                # If there are no more tracks, break out of the loop
                if not tracks:
                    break

                # Append tracks to the all_tracks list
                all_tracks.extend(tracks)

                # Get the URL for the next page, if available
                next_page_url = data.get("next")

                # If there's no next page, break out of the loop
                if not next_page_url:
                    break

                # Parse the "after" parameter from the next page URL to use in the next request
                next_page_params = parse_qs(urlparse(next_page_url).query)
                after_timestamp = next_page_params.get("after", [])[0]

                # Update the timestamp for the next page request
                yesterday_unix_timestamp = after_timestamp

            except Exception as e:
                print("Error while processing Spotify API response:", str(e))
                return None  # Return None to indicate an error.

        # Extract relevant data from all_tracks
        tracks_ids = []
        played_at_list = []
        timestamps = []

        for track in all_tracks:
            tracks_ids.append(track["track"]["id"])
            played_at_list.append(track["played_at"])
            timestamps.append(track["played_at"][0:10])

        # Prepare a dictionary in order to turn it into a pandas dataframe below
        track_dict = {
            "track_id": tracks_ids,
            "played_at": played_at_list,
            "timestamp": timestamps
        }
        track_df = pd.DataFrame(track_dict, columns=["track_id", "played_at", "timestamp"])

        print("1/3: Recently Played Tracks Data Extraction Successful!")

        return track_df

    # Method to extract track details
    def extract_track_details(self, track_df):
        if track_df is None:
            return None

        headers = self.get_spotify_headers()

        # Extract unique track IDs from the DataFrame
        track_ids = track_df["track_id"].unique()

        # Initialize a list to store track details
        track_details_list = []

        for track_id in track_ids:
            # Send a request to the Spotify API to get track details for the current track_id
            url = f"{self.tracks_details_url}/{track_id}"
            r = self.http_client.get(url, headers=headers)

            if r.status_code != 200:
                print("Failed to fetch data from Spotify API. Status code:", r.status_code)
                return None  # Return None to indicate an error.
            
            try: 
                track_data = r.json() 

                # Extract relevant details from the track_data
                track_details = {
                    "track_id": track_id,
                    "track_name": track_data["name"],
                    "artist_name": track_data["artists"][0]["name"], #This is the first artist in the list
                    # "artist_genres": ", ".join(track_data["artists"][0]["genres"]),
                    "album_name": track_data["album"]["name"],
                    "release_date": track_data["album"]["release_date"],
                    "length": track_data["duration_ms"],
                    "popularity": track_data["popularity"],
                    "explicit": track_data["explicit"],
                    "type": track_data["type"]
                }
                
                track_details_list.append(track_details)

            except Exception as e:
                print("Error while processing Spotify API response:", str(e))
                return None  # Return None to indicate an error.

        # Specify the column order for the DataFrame
        columns = [
            "track_id",
            "track_name",
            "artist_name",
            # "artist_genres",
            "album_name",
            "release_date",
            "length",
            "popularity",
            "explicit",
            "type"
        ]

        # Convert the list of dictionaries to a DataFrame with the specified column order
        track_details_df = pd.DataFrame(track_details_list, columns=columns)

        print("2/3: Track Details Extraction Successful!")

        return track_details_df

    # Method to extract track features
    def extract_track_features(self, track_df):
        if track_df is None:
            return None

        headers = self.get_spotify_headers()

        # Extract unique track IDs from the DataFrame
        track_ids = track_df["track_id"].unique()

        # Initialize a list to store track details
        track_features_list = []

        for track_id in track_ids:
            # Send a request to the Spotify API to get track details for the current track_id
            url = f"{self.tracks_features_url}/{track_id}"
            r = self.http_client.get(url, headers=headers)

            if r.status_code != 200:
                print("Failed to fetch data from Spotify API. Status code:", r.status_code)
                return None  # Return None to indicate an error.
            
            try: 
                track_features_data = r.json() 

                # Extract relevant features from the track_features_data
                track_features = {
                    "track_id": track_id,
                    "danceability": track_features_data["danceability"],
                    "duration_ms": track_features_data["duration_ms"],
                    "energy": track_features_data["energy"],
                    "acousticness": track_features_data["acousticness"],
                    "instrumentalness": track_features_data["instrumentalness"],
                    "key": track_features_data["key"],
                    "liveness": track_features_data["liveness"],
                    "loudness": track_features_data["loudness"],
                    "mode": track_features_data["mode"],
                    "speechiness": track_features_data["speechiness"],
                    "tempo": track_features_data["tempo"],
                    "time_signature": track_features_data["time_signature"],
                    "valence": track_features_data["valence"]
                }

                track_features_list.append(track_features)

            except Exception as e:
                print("Error while processing Spotify API response:", str(e))
                return None  # Return None to indicate an error.

        # Specify the column order for the DataFrame
        columns = [
            "track_id",
            "danceability",
            "duration_ms",
            "energy",
            "acousticness",
            "instrumentalness",
            "key",
            "liveness",
            "loudness",
            "mode",
            "speechiness",
            "tempo",
            "time_signature",
            "valence"
        ]

        # Convert the list of dictionaries to a DataFrame with the specified column order
        track_features_df = pd.DataFrame(track_features_list, columns=columns)

        print("3/3: Track Features Extraction Successful!")

        return track_features_df

    # Method to transform data (not currently used in the code)
    def transform_data(self, track_details_df):
        if track_details_df is None or track_details_df.empty:
            print("No data to transform.")
            return None

        # Flatten artist genres by joining them with a comma
        track_details_df["artist_genres"] = track_details_df["artist_genres"].apply(lambda x: ", ".join(x))

        print("Data Transformation Successful!")

        return track_details_df

    # Method to perform data quality checks
    def data_quality(self, load_df, primary_key_column):
        # Checking Whether the DataFrame is empty
        if load_df.empty:
            print('DataFrame is empty.')
            return False

        # Enforcing Primary Keys since we don't need duplicates
        if pd.Series(load_df[primary_key_column]).is_unique:
            pass
        else:
            # Raise an exception if primary key constraint is violated
            raise Exception("Primary Key Exception, Data Might Contain Duplicates")

        # Checking for Nulls in our data frame
        if load_df.isnull().values.any():
            raise Exception("Null Values Found")

        print("Data Quality Checks Passed!")

    # Method for the Spotify ETL process to extract recently played tracks
    def spotify_tracks_etl(self):
        tracks_df = self.extract_recently_played_tracks()
        self.data_quality(tracks_df, 'played_at')
        return tracks_df

    # Method for the Spotify ETL process to extract track details
    def spotify_details_etl(self, tracks_df):
        track_details_df = self.extract_track_details(tracks_df)
        self.data_quality(track_details_df, 'track_id')
        #track_details_df = self.transform_data(track_details_df)
        return track_details_df

    # Method for the Spotify ETL process to extract track features
    def spotify_features_etl(self, tracks_df):
        track_features_df = self.extract_track_features(tracks_df)
        self.data_quality(track_features_df, 'track_id')
        print("ETL Process Completed Successfully!")
        return track_features_df

def main():
    load_dotenv()

    config = SpotifyConfig(
        os.getenv("SPOTIFY_CLIENT_ID"),
        os.getenv("SPOTIFY_CLIENT_SECRET"),
        os.getenv("SPOTIFY_REFRESH_TOKEN"),
        os.getenv("SPOTIFY_ACCESS_TOKEN"),
        os.getenv("SPOTIFY_TOKEN_EXPIRATION"),
        "https://accounts.spotify.com/api/token"
    )

    token_manager = SpotifyTokenManager(config, HttpClient())

    etl = SpotifyETL(
        token_manager,
        recent_tracks_url="https://api.spotify.com/v1/me/player/recently-played", 
        tracks_details_url="https://api.spotify.com/v1/tracks", 
        tracks_features_url="https://api.spotify.com/v1/audio-features"
        )

    tracks_df = etl.spotify_tracks_etl()
    if tracks_df is not None:
        details_df = etl.spotify_details_etl(tracks_df)
        features_df = etl.spotify_features_etl(tracks_df)

    print("Here are the First 5 Rows of DataFrame 'tracks_df':")
    print(tracks_df.head())

    print("Here are the First 5 Rows of DataFrame 'details_df':")
    print(details_df.head())

    print("Here are the First 5 Rows of DataFrame 'features_df':")
    print(features_df.head())

if __name__ == "__main__":
    main()