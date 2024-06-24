from backend.data_analyzer import build_query


def test_build_query_all_all():
    actual_query = build_query(genre='All', year='All')
    expected_query = '''
        SELECT DISTINCT title, MAX(popularity) OVER (PARTITION BY title) AS popularity, release_date, poster_path FROM movies
        WHERE 1=1 
        ORDER BY popularity DESC
        LIMIT 10
    '''
    assert actual_query == expected_query


def test_build_query_all_2020():
    actual_query = build_query(genre='All', year=2020)
    expected_query = '''
        SELECT DISTINCT title, MAX(popularity) OVER (PARTITION BY title) AS popularity, release_date, poster_path FROM movies
        WHERE 1=1 AND year=2020 
        ORDER BY popularity DESC
        LIMIT 10
    '''
    assert actual_query == expected_query


def test_build_query_action_all():
    actual_query = build_query(genre='Action', year='All')
    expected_query = '''
        SELECT DISTINCT title, MAX(popularity) OVER (PARTITION BY title) AS popularity, release_date, poster_path FROM movies
        WHERE 1=1 AND genre='Action' 
        ORDER BY popularity DESC
        LIMIT 10
    '''
    assert actual_query == expected_query


def test_build_query_action_2020():
    actual_query = build_query(genre='Action', year=2020)
    expected_query = '''
        SELECT DISTINCT title, MAX(popularity) OVER (PARTITION BY title) AS popularity, release_date, poster_path FROM movies
        WHERE 1=1 AND genre='Action' AND year=2020 
        ORDER BY popularity DESC
        LIMIT 10
    '''
    assert actual_query == expected_query
