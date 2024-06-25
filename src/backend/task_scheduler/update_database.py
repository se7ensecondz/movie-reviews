import os
import time
from datetime import datetime, timedelta

import duckdb
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.backend.database.utils import DB
from src.backend.tmdb_data_collector.genres import insert_genres
from src.backend.tmdb_data_collector.movies import insert_movies

conn = duckdb.connect(DB)
headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {os.environ['BEARER_TOKEN']}"
}
now = datetime.now()
default_args = {
    'start_date': now - timedelta(days=1),
    'retries': 3,
}
dag = DAG('movie-database-update', default_args=default_args, schedule_interval='0 0 * * *')


# Schedule to update genre and movie databases daily
def update_genres():
    url = "https://api.themoviedb.org/3/genre/movie/list?language=en"
    response = requests.get(url, headers=headers)
    genres = response.json().get('genres')
    insert_genres(conn, genres)


update_genres_task = PythonOperator(task_id='update_genres', python_callable=update_genres, dag=dag)


def update_movies():
    year = now.year
    genre_ids = dict(conn.query("""SELECT * FROM genres""").fetchall())
    for genre, genre_id in genre_ids.items():
        url = (
            f"https://api.themoviedb.org/3/discover/movie?include_adult=false&include_video=false&language=en-US"
            f"&page=1"
            f"&primary_release_year={year}&sort_by=popularity.desc&with_genres={genre_id}")
        response = requests.get(url, headers=headers)
        top15_popular = response.json()['results'][:15]
        insert_movies(conn, genre, year, top15_popular)
        time.sleep(1)


update_movies_task = PythonOperator(task_id='update_movies', python_callable=update_movies, dag=dag)


def cleanup():
    conn.commit()
    conn.close()


cleanup_task = PythonOperator(task_id='cleanup', python_callable=cleanup, dag=dag)

# DAG
update_genres_task >> update_movies_task >> cleanup_task
