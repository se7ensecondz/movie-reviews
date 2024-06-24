import duckdb

from datetime import datetime

from backend.database.utils import DB


def get_years() -> list[int]:
    return list(range(2019, datetime.now().year + 1))


def get_genre_ids() -> dict:
    conn = duckdb.connect(DB)
    genres = dict(conn.query("""SELECT * FROM genres""").fetchall())
    conn.close()
    return genres
