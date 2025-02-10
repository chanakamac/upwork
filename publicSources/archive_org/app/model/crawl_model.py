from pydantic import BaseModel
from datetime import datetime

class CrawlModel(BaseModel):
    url: str
    crawl_sdate: datetime
    crawl_edate: datetime
