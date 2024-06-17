import os
import time

import duckdb
import requests

from src.backend.database.utils import DB
from src.backend.utils import get_years, get_genre_ids


def get_movie(conn, year, genre, genre_id):
    url = (f"https://api.themoviedb.org/3/discover/movie?include_adult=false&include_video=false&language=en-US&page=1"
           f"&primary_release_year={year}&sort_by=popularity.desc&with_genres={genre_id}")
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI2MTNjZjYzYzE3YTRlNjU4OTQ5MDM3NmE1M2EyZmU3ZiIsInN1YiI6IjY2NjRiZTYxMGE2YTRjN2FkMWU5OTdlNCIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.XzPdW6GSVc-zhM1WgxDcTQWzUPyUTIRyiurHv0TLSEc"
    }
    response = requests.get(url, headers=headers)
    top15_popular = response.json()['results'][:15]
    for movie in top15_popular:
        movie_id = movie['id']
        movie_title = movie['title']
        movie_genre = genre
        movie_popularity = movie['popularity']
        movie_year = year
        movie_release_date = movie['release_date']
        poster_path = movie.get('poster_path')
        if poster_path is not None:
            movie_poster_path = "https://image.tmdb.org/t/p/w200" + poster_path
        else:
            movie_poster_path = None
        conn.execute(
            f"""
                INSERT OR REPLACE INTO movies VALUES (
                    {movie_id}, 
                    '{movie_title.replace("'", "''")}', 
                    '{movie_genre}', 
                    {movie_popularity}, 
                    {movie_year}, 
                    '{movie_release_date}', 
                    '{movie_poster_path}'
                )
            """
        )
    conn.commit()


def create_movies_table(conn):
    conn.execute(
        f"""
            CREATE TABLE IF NOT EXISTS movies (id INTEGER, title VARCHAR, genre VARCHAR, 
            popularity FLOAT, year INTEGER, release_date DATE, poster_path VARCHAR, PRIMARY KEY (id, genre))
        """
    )
    conn.commit()


def get_movies(years, genre, genre_id):
    for year in years:
        get_movie(year, genre, genre_id)
        time.sleep(1)


if __name__ == '__main__':
    years = get_years()
    genre_ids = get_genre_ids()

    conn = duckdb.connect(DB)
    create_movies_table(conn)
    for genre, genre_id in genre_ids.items():
        for year in years:
            get_movie(conn, year, genre, genre_id)
            time.sleep(1)
    conn.close()
