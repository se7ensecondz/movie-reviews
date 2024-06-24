import duckdb
import pytest
from datetime import datetime

from backend.tmdb_data_collector.movies import drop_movies_table, create_movies_table, insert_movies

TEST_DB = 'unit_test.duckdb'
conn = duckdb.connect(TEST_DB)


@pytest.mark.order(1)
def test_create_movies_table():
    create_movies_table(conn)
    tables = conn.query("SHOW TABLES").fetchall()
    assert len(tables) == 1
    assert tables[0][0] == 'movies'
    drop_movies_table(conn)


@pytest.mark.order(2)
def test_drop_movies_table():
    drop_movies_table(conn)
    tables = conn.query("SHOW TABLES").fetchall()
    assert len(tables) == 0


@pytest.mark.order(3)
def test_insert_into_genres():
    drop_movies_table(conn)
    create_movies_table(conn)

    year = 2020
    genre = 'Action'

    mock_movies = [
        {'id': 42, 'title': 'movie1', 'popularity': 501, 'release_date': datetime(year=year, month=1, day=1).date(),
         'poster_path': '/path1'},
        {'id': 43, 'title': 'movie2', 'popularity': 502, 'release_date': datetime(year=year, month=6, day=30).date(),
         'poster_path': '/path2'},
    ]
    insert_movies(conn, genre, year, mock_movies)
    actual_movies = conn.query("SELECT * FROM movies").fetchall()
    actual_movies = sorted(actual_movies, key=lambda x: x[0], reverse=False)
    expected_movies = [
        (42, 'movie1', 'Action', 501.0, 2020, datetime(year=2020, month=1, day=1).date(),
         'https://image.tmdb.org/t/p/w200/path1'),
        (43, 'movie2', 'Action', 502.0, 2020, datetime(year=2020, month=6, day=30).date(),
         'https://image.tmdb.org/t/p/w200/path2')
    ]
    assert actual_movies == expected_movies
    drop_movies_table(conn)
