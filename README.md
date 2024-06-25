# Movie Reviews Platform

## Components

- **Web application basic form and Reporting**

    The web application is hosted on Heroku at https://movie-reviews-project-d18010416a93.herokuapp.com/, please see `movie-reviews-platform.pdf` for the design and system requirements. Currently, users can specify genre and release year, and the app will return the most popular movies correspondingly.


- **Data Collector and Analyzer**

    These are located at `src/backend/tmdb_data_collector` and `src/backend/data_analyzer`. As of now, we are collecting genre and movie data by using TMDB public API, analytics include the most/least popular movies per genre and release year, and which genre is more/less popular than the others. We will use `Apache Airflow` to schedule the data collection task to run daily.


- **Unit and Integration Tests**

    The project uses a conventional `src/test` repository structure, where `src` contains all source code and `test` contains all unit and integration tests. Our integration tests focuses on interacting with TMDB public API and populating a local DuckDB database.


-  **Data Persistence**

    For elegance and simplicity, we used a local `DuckDB` database for data storage, DuckDB is perfect for analytics type of queries.


- **Backend APIs, Monitoring and Metrics**

    Backend APIs are built using `FastAPI`, a highly-performant and popular Python API framework. FastAPI has elegant integration with `Prometheus`, making monitoring and metrics collection much easier. We have a `/healthcheck` endpoint for application health check and `/metrics` endpoint for collecting metrics.


- **CI/CD**

    We used `GitHub Actions` for continuous integration. PR validations include code linting and pass of integration tests. When the PR is merged into `main`, the latest version of application will be automatically deployed on `Heroku`.

# Development
- run integration test with `pytest test/integration_test.py` command
