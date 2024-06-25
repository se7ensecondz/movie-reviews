def _get_filters(genre, year):
    genre_filter = '' if genre == "All" else f"AND genre='{genre}' "
    year_filter = '' if year == "All" else f"AND year={year} "
    return 'WHERE 1=1 ' + genre_filter + year_filter


def find_top10_of_genre_year(genre, year, descending=True):
    order = 'DESC' if descending else 'ASC'
    return f"""
        SELECT DISTINCT title, MAX(popularity) OVER (PARTITION BY title) AS popularity, release_date, poster_path FROM movies
        {_get_filters(genre, year)}
        ORDER BY popularity {order}
        LIMIT 10
    """


def most_popular_of_genre_year(genre, year):
    return find_top10_of_genre_year(genre, year, descending=True)


def least_popular_of_genre_year(genre, year):
    return find_top10_of_genre_year(genre, year, descending=False)


def average_popularity_of_genre(genre):
    return f"""
        SELECT MEAN(popularity) FROM movies
        WHERE 1=1
        AND genre='{genre}'
        GROUP BY genre
    """
