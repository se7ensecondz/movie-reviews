import duckdb
import pytest

from src.backend.tmdb_data_collector.genres import create_genres_table, drop_genres_table, insert_into_genres

TEST_DB = 'unit_test.duckdb'
conn = duckdb.connect(TEST_DB)


@pytest.mark.order(1)
def test_create_genres_table():
    create_genres_table(conn)
    tables = conn.query("SHOW TABLES").fetchall()
    assert len(tables) == 1
    assert tables[0][0] == 'genres'
    drop_genres_table(conn)


@pytest.mark.order(2)
def test_drop_genres_table():
    drop_genres_table(conn)
    tables = conn.query("SHOW TABLES").fetchall()
    assert len(tables) == 0


@pytest.mark.order(3)
def test_insert_into_genres():
    drop_genres_table(conn)
    create_genres_table(conn)
    mock_genres = [
        {'id': 42, 'name': 'Action'},
        {'id': 43, 'name': 'Comedy'},
        {'id': 44, 'name': 'Foobar'},  # should not be inserted
    ]
    insert_into_genres(conn, mock_genres)
    actual_genres = conn.query("SELECT * FROM genres").fetchall()
    actual_genres = sorted(actual_genres, key=lambda x: x[1], reverse=False)
    expected_genres = [('Action', 42), ('Comedy', 43)]
    assert actual_genres == expected_genres
    drop_genres_table(conn)
