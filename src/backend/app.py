import duckdb
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from src.backend.utils import get_years, get_genre_ids, get_table_name

app = FastAPI()


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
    conn = duckdb.connect('src/backend/database/dev.duckdb')
    movies = conn.execute(f"""
        SELECT title, popularity, release_date, poster_path FROM {get_table_name(genre)}
        WHERE year={year}
        ORDER BY popularity DESC
        LIMIT 10
    """).fetchall()

    html_content = f'<h2>Top 10 {genre} Movies of {year}</h2> <br><br>'
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

