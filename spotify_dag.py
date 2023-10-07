import datetime as dt
import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from spotify_token import SpotifyTokenManager, SpotifyConfig, HttpClient
from spotify_etl import SpotifyETL

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dt.datetime(2023, 9, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1)
}

dag = DAG(
    'spotify_dag',
    default_args=default_args,
    description='Spotify ETL process 1-min',
    schedule_interval=dt.timedelta(minutes=60),
)

def ETL():
    # Defining the connection to the database
    conn = BaseHook.get_connection('elephant_sql')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}/{conn.schema}')
    
    # Running the ETL functions
    print("Started")

    # Loading environment variables
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

    # Create a SpotifyTokenManager object with the configuration and HTTP clien
    token_manager = SpotifyTokenManager(config, HttpClient())

    # Create a SpotifyETL object with token manager and endpoints
    etl = SpotifyETL(
        token_manager,
        recent_tracks_url="https://api.spotify.com/v1/me/player/recently-played", 
        tracks_details_url="https://api.spotify.com/v1/tracks", 
        tracks_features_url="https://api.spotify.com/v1/audio-features"
        )

    # Extracting the tracks
    tracks_df = etl.spotify_tracks_etl()
    tracks_df.to_sql('recent_tracks', engine, if_exists='replace')

    # Extracting the details and features
    if tracks_df is not None:
        details_df = etl.spotify_details_etl(tracks_df)
        details_df.to_sql('track_details', engine, if_exists='replace')

        features_df = etl.spotify_features_etl(tracks_df)
        features_df.to_sql('track_features', engine, if_exists='replace')

    # Print the final message
    print("Database updated successfully!")

with dag:    
    # Task to create recent_tracks table on PostgreSQL database
    create_tracks_table= PostgresOperator(
        task_id='create_tracks_table',
        postgres_conn_id='elephant_sql',
        sql="""
            CREATE TABLE IF NOT EXISTS recent_tracks(
            track_id VARCHAR(200) PRIMARY KEY,
            played_at VARCHAR(200),
            timestamp VARCHAR(200),
            FOREIGN KEY (track_id) REFERENCES recent_tracks(track_id)
        )
        """
    )

    # Task to create track_details table on PostgreSQL database
    create_details_table= PostgresOperator(
        task_id='create_details_table',
        postgres_conn_id='elephant_sql',
        sql="""
            CREATE TABLE IF NOT EXISTS track_details(
            track_id VARCHAR(200) PRIMARY KEY,
            track_name VARCHAR(200),
            artist_name VARCHAR(200),
            album_name VARCHAR(200),
            release_date VARCHAR(200),
            length INTEGER,
            popularity INTEGER,
            explicit BOOLEAN,
            type VARCHAR(50)
        )
        """
    )

    # Task to create track_features table on PostgreSQL database
    create_features_table= PostgresOperator(
        task_id='create_features_table',
        postgres_conn_id='elephant_sql',
        sql="""
            CREATE TABLE IF NOT EXISTS track_features(
            track_id VARCHAR(200) PRIMARY KEY,
            danceability FLOAT,
            duration_ms INTEGER,
            energy FLOAT,
            acousticness FLOAT,
            instrumentalness FLOAT,
            key INTEGER,
            liveness FLOAT,
            loudness FLOAT,
            mode INTEGER,
            speechiness FLOAT,
            tempo FLOAT,
            time_signature INTEGER,
            valence FLOAT
        )
        """
    )

    # Task to run Python ETL
    run_etl = PythonOperator(
        task_id='spotify_etl',
        python_callable=ETL,
        dag=dag,
    )

    # Task execution order
    create_tracks_table >> create_details_table >> create_features_table >> run_etl