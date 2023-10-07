import requests
import os
from dotenv import load_dotenv, set_key
import base64
from datetime import datetime, timedelta

# Class to manage Spotify access tokens
class SpotifyTokenManager:
    def __init__(self, config, http_client):
        self.config = config
        self.http_client = http_client

    # Method to refresh the access token
    def refresh_access_token(self):
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {base64.b64encode((self.config.client_id + ':' + self.config.client_secret).encode()).decode()}"
        }

        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.config.refresh_token,
        }

        # Send a POST request to Spotify's token endpoint
        response = self.http_client.post(self.config.token_url, headers=headers, data=data)

        if response.status_code == 200:
            token_data = response.json()
            access_token = token_data["access_token"]
            expires_in = token_data["expires_in"]

            # Calculate the expiration time for the access token
            expiration_time = datetime.now() + timedelta(seconds=expires_in)

            # Update the access token and expiration time
            self.config.update_access_token(access_token, expiration_time)

            return access_token
        else:
            print("Failed to refresh the access token. Status code:", response.status_code)
            return None

    # Method to get the access token
    def get_access_token(self):
        if self.config.access_token and self.config.is_access_token_valid():
            print(f"Access token is still valid, expires at: {self.config.token_expiration}")
            return self.config.access_token

        elif self.config.access_token:
            print("Access token is present but expired. Refreshing...")
            access_token = self.refresh_access_token()
            if access_token:
                print(f"Access token refreshed. New expiration time: {self.config.token_expiration}")
                return access_token

        else:
            print("No access token found. Generating a new access token!")
            access_token = self.refresh_access_token()
            if access_token:
                print(f"Access token generated. Expiration time: {self.config.token_expiration}")
                return access_token

        return None

# Class to manage Spotify API configuration
class SpotifyConfig:
    def __init__(self, client_id, client_secret, refresh_token, access_token, token_expiration, token_url):
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.access_token = access_token
        if token_expiration:
            self.token_expiration = datetime.fromtimestamp(float(token_expiration))
        else:
            self.token_expiration = None
        self.token_url = token_url

    # Method to update the access token and its expiration time
    def update_access_token(self, access_token, expiration_time):
        self.access_token = access_token
        self.token_expiration = expiration_time
        self.update_env_file(access_token, expiration_time)

    # Method to check if the access token is valid
    def is_access_token_valid(self):
        if self.access_token and self.token_expiration and datetime.now() < self.token_expiration:
            return True
        return False

    # Method to update the environment file with the new access token and expiration time
    def update_env_file(self, access_token, expiration_time):
        # This needs to be updated to ensure that the .env file is being updated within the Docker container!
        set_key(".env", "SPOTIFY_ACCESS_TOKEN", access_token)
        set_key(".env", "SPOTIFY_TOKEN_EXPIRATION", str(expiration_time.timestamp()))

# HTTP client class for making API requests
class HttpClient:
    def post(self, url, headers, data):
        return requests.post(url, headers=headers, data=data)
    
    def get(self, url, headers):
        return requests.get(url, headers=headers)

def main():
    load_dotenv()

    # Create a SpotifyConfig object with configuration values from environment variables
    config = SpotifyConfig(
        os.getenv("SPOTIFY_CLIENT_ID"),
        os.getenv("SPOTIFY_CLIENT_SECRET"),
        os.getenv("SPOTIFY_REFRESH_TOKEN"),
        os.getenv("SPOTIFY_ACCESS_TOKEN"),
        os.getenv("SPOTIFY_TOKEN_EXPIRATION"),
        "https://accounts.spotify.com/api/token"
    )

    # Create an HttpClient object for making API requests
    http_client = HttpClient()

    # Create a SpotifyTokenManager object with the configuration and HTTP client
    token_manager = SpotifyTokenManager(config, http_client)

    # Get the access token from the token manager
    access_token = token_manager.get_access_token()

    if access_token:
        # Print a masked version of the access token, displaying only the first few characters
        masked_token = access_token[:3] + "..." 
        print(f"Access token obtained: {masked_token}")
    else:
        print("Failed to obtain a valid access token.")

    return access_token

if __name__ == "__main__":
    main()