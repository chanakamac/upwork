from fastapi import APIRouter, Query
from typing import Optional
from app.service.crawler_service import process_crawler_data

router = APIRouter()

# Define a route that takes a URL and a date as query parameters
@router.get("/archive")
async def get_archive(url: str, start_date: str, end_date: str):
    result = await process_crawler_data(url, start_date, end_date)
    return {"result": result}