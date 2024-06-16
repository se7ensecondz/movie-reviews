import requests

import duckdb

url = "https://api.themoviedb.org/3/genre/movie/list?language=en"
headers = {
    "accept": "application/json",
    "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI2MTNjZjYzYzE3YTRlNjU4OTQ5MDM3NmE1M2EyZmU3ZiIsInN1YiI6IjY2NjRiZTYxMGE2YTRjN2FkMWU5OTdlNCIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.XzPdW6GSVc-zhM1WgxDcTQWzUPyUTIRyiurHv0TLSEc"
}
response = requests.get(url, headers=headers)
genres = response.json().get('genres')


def drop_genres():
    conn.execute("""
        DROP TABLE IF EXISTS genres
    """)


def create_genres():
    conn.execute("""CREATE TABLE IF NOT EXISTS genres(name VARCHAR PRIMARY KEY, id INTEGER)""")


def insert_genres():
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


if __name__ == '__main__':
    conn = duckdb.connect('src/backend/database/dev.duckdb')
    drop_genres()
    create_genres()
    insert_genres()
    conn.commit()
    conn.close()
