
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.controller.crawler_controller import router
# from app.service.crawler_service import scheduler
from bs4 import BeautifulSoup



app = FastAPI(
    description='Web Crawler API Service',
    version="1.0",
    title='Web Crawler API'
)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    allow_origins=["*"]
)

app.include_router(router)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)