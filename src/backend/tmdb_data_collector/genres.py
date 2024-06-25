def drop_genres_table(conn):
    conn.execute("""
        DROP TABLE IF EXISTS genres
    """)
    conn.commit()


def create_genres_table(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS genres(name VARCHAR PRIMARY KEY, id INTEGER)""")
    conn.commit()


def insert_genres(conn, genres):
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
            conn.execute(f"""INSERT OR REPLACE INTO genres VALUES ('{genre_name}', {genre_id})""")
    conn.commit()
