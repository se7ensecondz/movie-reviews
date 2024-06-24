from fastapi import APIRouter
from starlette.responses import PlainTextResponse

healthcheck_router = APIRouter()


@healthcheck_router.get('/healthcheck')
def healthcheck():
    return PlainTextResponse(content="OK", status_code=200)
