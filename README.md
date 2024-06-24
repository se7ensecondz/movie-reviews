# Movie Reviews Platform

1. Navigate to `src/backend` folder

2. `database/dev.duckdb` is the local DuckDB for data storage

3. `tmdb/genres.py` uses public TMDB API to fetch genres data, including `name` and `id`

4. `tmdb/movies.py` uses public TMDB API to fetch movies data, including `id`, `title`, `genre`, `popularity`, `release date` and `poster path`

heroku link: https://movie-reviews-project-d18010416a93.herokuapp.com/

# Development
- fill JWT token in `.env.template` and rename it to `.env`, then run integration test with it