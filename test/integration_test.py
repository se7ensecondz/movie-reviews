import os
import time

import duckdb
import requests

from backend.tmdb_data_collector.genres import drop_genres_table, create_genres_table, insert_into_genres
from backend.tmdb_data_collector.movies import drop_movies_table, create_movies_table, insert_movies
from backend.utils import get_years, get_genre_ids

TEST_DB = 'integration_test.duckdb'
conn = duckdb.connect(TEST_DB)


def test_integration_test():
    # 1. create and populate genre table
    url = "https://api.themoviedb.org/3/genre/movie/list?language=en"
    headers = {
        "accept": "application/json",
        "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI2MTNjZjYzYzE3YTRlNjU4OTQ5MDM3NmE1M2EyZmU3ZiIsInN1YiI6IjY2NjRiZTYxMGE2YTRjN2FkMWU5OTdlNCIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.XzPdW6GSVc-zhM1WgxDcTQWzUPyUTIRyiurHv0TLSEc"
    }
    response = requests.get(url, headers=headers)
    genres = response.json().get('genres')
    drop_genres_table(conn)
    create_genres_table(conn)
    insert_into_genres(conn, genres)

    # 2. create and populate movie table
    years = get_years()
    genre_ids = get_genre_ids()
    drop_movies_table(conn)
    create_movies_table(conn)
    for genre, genre_id in genre_ids.items():
        for year in years:
            url = (
                f"https://api.themoviedb.org/3/discover/movie?include_adult=false&include_video=false&language=en-US"
                f"&page=1"
                f"&primary_release_year={year}&sort_by=popularity.desc&with_genres={genre_id}")
            response = requests.get(url, headers=headers)
            top15_popular = response.json()['results'][:15]
            insert_movies(conn, genre, year, top15_popular)
            time.sleep(1)

    # 3. assert movie data is retrieved correctly
    for genre, year in zip(get_genre_ids(), get_years()):
        movies_of_genre_year = conn.query(f"SELECT * FROM movies WHERE genre='{genre}' AND year={year}").fetchall()
        assert len(movies_of_genre_year) > 0

    # 3. clean up database
    drop_genres_table(conn)
    drop_movies_table(conn)
    conn.close()


os.remove(TEST_DB)
