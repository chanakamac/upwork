from fastapi import APIRouter
from starlette.responses import HTMLResponse

from app.controller.crawler_controller import router as crawl_router

all_routers = APIRouter()
all_routers.include_router(crawl_router)


@all_routers.get("/", response_class=HTMLResponse)
async def root():
    html_content = """
        <html>
            <head>
                <title>Web Crawler API Service</title>
            </head>
            <body>
                <h1>Welcome to Web Crawler API Service</h1>
            </body>
        </html>
        """
    return HTMLResponse(content=html_content)
