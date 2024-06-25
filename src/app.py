import duckdb
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from src.backend.prometheus_metrics import metrics_app
from src.backend.data_analyzer import most_popular_of_genre_year
from src.backend.database.utils import DB
from src.backend.healthcheck import healthcheck_router
from src.backend.utils import get_years, get_genre_ids

app = FastAPI(debug=False)
app.include_router(healthcheck_router)
app.mount("/metrics", metrics_app)


def build_options(*args):
    options = ''
    for arg in args:
        options += f"""<option value="{arg}">{arg}</option>"""
    return options


@app.get("/")
def main():
    html_content = f'''
    <form action="/search" method="GET">
    <b>Select Genre:</b>
    <select name="genre" id="genre">
    {build_options('All', *get_genre_ids())}
    </select>
    
    <br></br>
    
    <b>Select Year:</b>
    <select name="year" id="year">
    {build_options('All', *get_years())}
    </select>

    <br></br>
    <input type="submit", value="Search">
    </form>

    <br></br>
    <i>Powered by FastAPI and DuckDB</i>
    '''
    return HTMLResponse(content=html_content, status_code=200)


@app.get('/search')
def search(genre, year):
    conn = duckdb.connect(DB)
    query = most_popular_of_genre_year(genre, year)
    movies = conn.execute(query).fetchall()

    genre = '' if genre == 'All' else genre
    year = 'All Time' if year == 'All' else year
    html_content = f'<h2>Top {genre} Movies of {year}</h2> <br><br>'
    html_content = ' '.join(html_content.split())
    for m in movies:
        html_content += f"""
         <div style="vertical-align:middle" align="center">
           <div class="text-block">
            <b>{m[0]}</b>
            <br>
            <i>Score: {int(m[1])}</i>
            <br>
            <i>Release Date: {m[2]}</i>
          </div>
          <img src="{m[3]}">
        </div>
        <br>
        """

    return HTMLResponse(content=html_content)
