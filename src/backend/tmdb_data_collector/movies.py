def drop_movies_table(conn):
    conn.execute("""
        DROP TABLE IF EXISTS movies
    """)
    conn.commit()


def create_movies_table(conn):
    conn.execute(
        f"""
            CREATE TABLE IF NOT EXISTS movies (id INTEGER, title VARCHAR, genre VARCHAR, 
            popularity FLOAT, year INTEGER, release_date DATE, poster_path VARCHAR, PRIMARY KEY (id, genre))
        """
    )
    conn.commit()


def insert_movies(conn, genre, year, movies):
    for movie in movies:
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
                INSERT OR REPLACE INTO movies VALUES (
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
    conn.commit()
