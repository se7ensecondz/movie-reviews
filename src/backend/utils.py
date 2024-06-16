from datetime import datetime


def get_years() -> list[int]:
    return list(range(2019, datetime.now().year + 1))


def get_genre_ids() -> dict:
    import duckdb
    conn = duckdb.connect('src/backend/database/dev.duckdb')
    return dict(conn.query("""SELECT * FROM genres""").fetchall())


def get_table_name(genre):
    return genre.strip().replace(' ', '')
