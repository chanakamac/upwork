import requests
from datetime import datetime
import os
from datetime import datetime
from bs4 import BeautifulSoup
import html5lib
import lxml
 

WAYBACK_API_URL = "http://web.archive.org/cdx/search/cdx"
save_dir = 'snapshots'

async def process_crawler_data(url: str, sdate: str, edate: str):
    try:
        crawl_sdate = datetime.strptime(sdate, "%Y-%m-%d")
    except ValueError:
        return {"error": "Invalid date format. Please use YYYY-MM-DD format."}
    
    try:
        crawl_edate = datetime.strptime(edate, "%Y-%m-%d")
    except ValueError:
        return {"error": "Invalid date format. Please use YYYY-MM-DD format."}
 
    # result = get_wayback_snapshots(url, crawl_sdate.strftime("%Y%m%d"), crawl_edate.strftime("%Y%m%d"))

    result = download_website_snapshots(url, crawl_sdate.strftime("%Y%m%d"), crawl_edate.strftime("%Y%m%d"), save_dir)
 
    return result


def get_wayback_snapshots(url, start_date, end_date):
     
    params = {
        'url': url,
        'from': start_date,
        'to': end_date,
        'output': 'json',
        'fl': 'timestamp,original',
        'filter': 'statuscode:200',
        'collapse': 'digest',
    }

    response = requests.get(WAYBACK_API_URL, params=params)

    response.raise_for_status()
     
    data = response.json()

    print(data)
    
    if len(data) < 2:
        print("No snapshots found for the given date range.")
        return []
     
    return data[1:]


def download_html(snapshot_url, timestamp, save_dir):

    # url = 'https://web.archive.org/web/*/http://bankier.pl/'

    # r = requests.get(url)
    # html_content = r.text
    # soup = BeautifulSoup(html_content, 'lxml')  
    # links = [a.get('href') for a in soup.find_all('a', href=True)]
    # print(links)



    # archive_url = f"https://web.archive.org/web/{timestamp}/{snapshot_url}"

    archive_url = f"https://web.archive.org/web/*/{snapshot_url}"

    response = requests.get(archive_url)

    print(response)

    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    filename = f"{timestamp}.html"
    file_path = os.path.join(save_dir, filename)
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(response.text)
    
    return file_path


def download_website_snapshots(url, start_date, end_date, save_dir):
     
    snapshots = get_wayback_snapshots(url, start_date, end_date)
     
     
    # downloaded_files = []
    # for snapshot in snapshots:
    #     timestamp, original_url = snapshot
    #     file_path = download_html(original_url, timestamp, save_dir)
    #     downloaded_files.append(file_path)
    
    # return downloaded_files

 
 
