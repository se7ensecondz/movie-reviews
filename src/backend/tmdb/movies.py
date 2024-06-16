import time

import duckdb
import requests

from src.backend.utils import get_years, get_genre_ids, get_table_name


def fetch_movie(year, genre, genre_id):
    url = (f"https://api.themoviedb.org/3/discover/movie?include_adult=false&include_video=false&language=en-US&page=1"
           f"&primary_release_year={year}&sort_by=popularity.desc&with_genres={genre_id}")
    headers = {
        "accept": "application/json",
        "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI2MTNjZjYzYzE3YTRlNjU4OTQ5MDM3NmE1M2EyZmU3ZiIsInN1YiI6IjY2NjRiZTYxMGE2YTRjN2FkMWU5OTdlNCIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.XzPdW6GSVc-zhM1WgxDcTQWzUPyUTIRyiurHv0TLSEc"
    }
    response = requests.get(url, headers=headers)
    top10_popular = response.json()['results'][:10]
    for movie in top10_popular:
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
                INSERT OR REPLACE INTO {get_table_name(genre)} VALUES (
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


def create_movie(genre):
    conn.execute(
        f"""
            CREATE TABLE IF NOT EXISTS {get_table_name(genre)}(id INTEGER PRIMARY KEY, title VARCHAR, genre VARCHAR, 
            popularity FLOAT, year INTEGER, release_date DATE, poster_path VARCHAR)
        """
    )


def fetch_movies(years, genre, genre_id):
    for year in years:
        fetch_movie(year, genre, genre_id)
        time.sleep(1)


if __name__ == '__main__':
    conn = duckdb.connect('src/backend/database/dev.duckdb')
    years = get_years()
    genre_ids = get_genre_ids()
    for g, gid in genre_ids.items():
        create_movie(g)
        fetch_movies(years, g, gid)
        time.sleep(1)
    conn.commit()
    conn.close()
