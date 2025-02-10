# app/controller/crawler_controller.py
from fastapi import APIRouter, Response, HTTPException
from app.model.crawler_model import CrawlModel
from app.service.crawler_service import CrawlService
import asyncio

router = APIRouter(prefix='/v1/api/web', tags=['Crawler'])
crawler_service = CrawlService()




@router.post("/crawler")
async def get_crawler(response: Response, crawl_model: CrawlModel):
    res_data = await crawler_service.cron_crawler(crawl_model=crawl_model)
    return {"res_data": res_data}

