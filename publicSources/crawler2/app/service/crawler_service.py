from app.model.crawler_model import CrawlModel
from app.entity.crawler_entity import Crawler
from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from crawlee.playwright_crawler import PlaywrightCrawler, PlaywrightCrawlingContext
from datetime import timedelta
from urllib.parse import urljoin, urlparse
from redis import Redis
import os
import uuid
import asyncio




class CrawlService:

    @classmethod
    async def crawl_link(cls, crawl_model:CrawlModel) -> None:
         
        try:
            #logger.info("Create Annotation Process Started")
            # crawler = Crawler(
            #     url = crawl_model.url
            # )
            url = crawl_model.url
            crone_details = crawl_model.cron_expression
            print("url $ crone_details",url,crone_details)   
            crawler = BeautifulSoupCrawler(
                max_request_retries=1,
                request_handler_timeout=timedelta(seconds=30),
                max_requests_per_crawl=10,
            )

            @crawler.router.default_handler
            async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
                # context.log.info(f'Processing {context.request.url} ...')
                original_domain = urlparse(context.request.url).netloc
                print("original domain ",original_domain)
                
                unique_links = set()
                links = [urljoin(context.request.url, a['href']) for a in context.soup.find_all('a', href=True)]
                valid_links = [link for link in links if link.startswith('http') and urlparse(link).netloc == original_domain and not(link in unique_links or unique_links.add(link))]
                print("############ get the all links in crawling ", valid_links)
                data = {
                    'url': context.request.url,
                    'links': valid_links,
                }
                await context.push_data(data)
    
            await crawler.run([url])
            data = await crawler.get_data()

            print(data)
            return data.items
       
        except Exception as ex:
            return None
        


    @classmethod
    async def crawl_content(cls,urls: any) -> None:
        crawler = PlaywrightCrawler(
            max_requests_per_crawl=len(urls),
            headless=True,
            browser_type='firefox',
        )
        print("all urls in get content2",urls)

        @crawler.router.default_handler
        async def request_handler(context: PlaywrightCrawlingContext) -> None:
            context.log.info(f'Processing {context.request.url} ...')
            try:
                # context.log.info(f'Processing {context.request.url} ...')
                page_content = await context.page.content()
                print('hellos')
                file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)),'content',f"index_{uuid.uuid4()}.html")
            
                with open(file_path,'w',encoding='utf=8') as file:
                    file.write(page_content)

            except Exception as e:
                context.log.error(f"Failed to process {context.request.url}: {str(e)}")
 
        try:
            
            await crawler.run(urls[0]['links'])
        except Exception as e:
            print(f"########## Crawler failed: {str(e)}")    
        

    @classmethod
    async def cron_crawler(cls,crawl_model:CrawlModel):  
        res_data = await cls.crawl_link(crawl_model=crawl_model)
        if not res_data:
            return {"message": "No data found"}
        print(f"Data crawled: {res_data}")

        await cls.crawl_content(res_data)
        return res_data
 