# 🎵 Spotify ETL Documentation 🎵

This documentation provides an overview of the Spotify ETL (Extract, Transform, Load) project, which includes Python scripts for extracting data from the Spotify API, transforming it, and loading it into a PostgreSQL database. This project is designed to automate the process of updating your recent tracks data from Spotify and serves as a foundation for future data analysis, visualization, and machine learning projects.

## Table of Contents

- [Introduction](#introduction)
- [Project Components](#project-components)
  - [spotify_token.py](#spotify_tokenpy)
  - [spotify_etl.py](#spotify_etlpy)
  - [spotify_dag.py](#spotify_dagpy)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Usage](#usage)
  - [Running the ETL Process](#running-the-etl-process)
  - [Airflow Integration](#airflow-integration)
- [Future Development](#future-development)
- [Acknowledgments](#acknowledgments)

## 🎶 Introduction 🎶

The Spotify ETL project is designed to automate the process of retrieving your recently played tracks from Spotify, enriching this data with additional details and features, and storing it in a PostgreSQL database. The project includes three Python scripts and is intended for users who want to maintain an up-to-date record of their Spotify listening history for further analysis.

## Project Components

### spotify_token.py

`spotify_token.py` contains the following classes:

- `SpotifyTokenManager`: Manages Spotify access tokens, refreshes tokens when needed, and handles token expiration.
- `SpotifyConfig`: Stores Spotify API configuration, including client ID, client secret, refresh token, and access token.
- `HttpClient`: Provides HTTP client functionality for sending requests to the Spotify API.

### spotify_etl.py

`spotify_etl.py` includes the `SpotifyETL` class, which is responsible for:

- Extracting recently played tracks from the Spotify API.
- Extracting track details and features for the extracted tracks.
- Performing data quality checks.
- Transforming the data (not being used for now).
- Storing the extracted data in DataFrames.

### spotify_dag.py

`spotify_dag.py` defines an Airflow DAG (Directed Acyclic Graph) for running the Spotify ETL process periodically. It includes tasks for creating database tables and running the ETL process. This script is designed to be used with Apache Airflow to automate data updates at regular intervals.

## Getting Started

### Prerequisites

Before using this project, ensure you have the following prerequisites:

- Python 3.7 or higher installed on your system.
- Access to a PostgreSQL database for storing Spotify data. You can use free instances available at [ElephantSQL](https://www.elephantsql.com/) for testing purposes.
- Spotify API credentials (client ID, client secret, refresh token) obtained by creating a Spotify Developer application. You can refer to [my other project](https://github.com/dasmedeiros/spotify-auth) where the process of obtaining and saving credentials in a `.env` file is facilitated.

### Installation

1. Clone the GitHub repository containing the project files:

   ```
   git clone https://github.com/yourusername/spotify-etl.git
   ```

2. Navigate to the project directory:

   ```
   cd spotify-etl
   ```

3. Install the required Python packages using pip:

   ```
   pip install -r requirements.txt
   ```

4. Configure your Spotify API credentials and database connection in the `.env` file. 

## Usage

### Running the ETL Process

To manually run the Spotify ETL process and update your Spotify data in the database, follow these steps:

1. Make sure you have completed the installation steps mentioned above.

2. Run the ETL process using the following command:

   ```
   python spotify_etl.py
   ```

   This command will extract your recently played tracks from Spotify, retrieve additional details and features, and store the data in the specified PostgreSQL database.

### Airflow Integration

This project is designed to be used with Apache Airflow for automating the ETL process. The `spotify_dag.py` script defines an Airflow DAG named "spotify_dag" that can be scheduled to run periodically.

To set up Apache Airflow and schedule the ETL process, refer to the [official Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/start/local.html) for local installation. For a reference implementation, you can also explore [Sidharth's project](https://github.com/sidharth1805/Spotify_etl), which includes Airflow integration.

## Future Development

This Spotify ETL project serves as a foundation for various future developments, including:

- **Cloud Deployment**: Consider deploying the ETL process on a cloud platform (e.g., AWS, Google Cloud) to automate data updates and reduce maintenance efforts.

- **Data Analysis**: Use the collected Spotify data for exploratory data analysis (EDA) to gain insights into your listening habits.

- **Data Visualization**: Create visualizations and dashboards to visualize your Spotify listening history.

- **Machine Learning**: Build machine learning models to make predictions or recommendations based on your Spotify data.

- **Additional Endpoints**: Explore and incorporate additional Spotify API endpoints to gather more data, such as user playlists and top tracks.

## Acknowledgments

This project is inspired by [Sidharth's Spotify ETL project](https://github.com/sidharth1805/Spotify_etl), and credit goes to Sidharth for the initial idea and implementation.
