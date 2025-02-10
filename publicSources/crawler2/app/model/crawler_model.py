from typing import Optional
from pydantic import BaseModel


class CrawlModel(BaseModel):
    url: str
    cron_expression: Optional[str] = None
    
