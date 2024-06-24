def build_query(genre, year):
    return f"""
        SELECT DISTINCT title, MAX(popularity) OVER (PARTITION BY title) AS popularity, release_date, poster_path FROM movies
        {_get_filters(genre, year)}
        ORDER BY popularity DESC
        LIMIT 10
    """


def _get_filters(genre, year):
    genre_filter = '' if genre == "All" else f"AND genre='{genre}' "
    year_filter = '' if year == "All" else f"AND year={year} "
    return 'WHERE 1=1 ' + genre_filter + year_filter
