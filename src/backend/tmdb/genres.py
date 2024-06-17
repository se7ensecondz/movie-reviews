import os

import duckdb
import requests

from src.backend.database.utils import DB

url = "https://api.themoviedb.org/3/genre/movie/list?language=en"
headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {os.environ.get('BEARER_TOKEN')}"
}
response = requests.get(url, headers=headers)
genres = response.json().get('genres')


def drop_genres_table(conn):
    conn.execute("""
        DROP TABLE IF EXISTS genres
    """)
    conn.commit()


def create_genres_table(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS genres(name VARCHAR PRIMARY KEY, id INTEGER)""")
    conn.commit()


def insert_into_genres(conn):
    interested_genres = {
        'Action',
        'Comedy',
        'Documentary',
        'Science Fiction',
        'Thriller',
    }
    for genre in genres:
        genre_id = int(genre['id'])
        genre_name = genre["name"]
        if genre_name in interested_genres:
            conn.execute(f"""INSERT INTO genres VALUES ('{genre_name}', {genre_id})""")
    conn.commit()


if __name__ == '__main__':
    conn = duckdb.connect(DB)
    drop_genres_table(conn)
    create_genres_table(conn)
    insert_into_genres(conn)
    conn.close()
