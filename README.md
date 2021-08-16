# spotify-airflow-etl
Spotify ETL for most recently played songs using Airflow and Docker.

In this project, I used requests lib for consulting the spotify API and psycopg2 to insert data into PostgreSQL and Apache Airflow for orchestrating the pipeline.

To use the spotify API it's necessary to get a fresh token and insert it into the dag file.
https://developer.spotify.com/console/get-recently-played/?limit=&after=&before=
