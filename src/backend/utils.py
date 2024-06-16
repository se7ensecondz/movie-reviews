from datetime import datetime

from src.backend.database.utils import DB


def get_years() -> list[int]:
    return list(range(2019, datetime.now().year + 1))


def get_genre_ids() -> dict:
    import duckdb
    conn = duckdb.connect(DB)
    return dict(conn.query("""SELECT * FROM genres""").fetchall())
