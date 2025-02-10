from fastapi import FastAPI
from app.controller.crawler_controller import router as crawler_router
 
app = FastAPI(
    description='Web archive API Service',
    version="1.0",
    title='Web archive API'
)
 
app.include_router(crawler_router)

@app.get("/")
def read_root():
    return {"message": "Crawler API is running!"}
 