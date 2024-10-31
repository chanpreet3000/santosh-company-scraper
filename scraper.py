import json
import os
import time
import urllib.parse
import uuid
import aiohttp
import asyncio

from db import Database
from Logger import Logger
from typing import Dict, List, Any, Tuple
from bs4 import BeautifulSoup


class ParallelScraper:
    def __init__(self, batch_size: int = 10, timeout: int = 10, max_retries: int = 3, batch_delay: int = 5):
        self.db = Database()
        self.batch_size = batch_size
        self.timeout = timeout
        self.max_retries = max_retries
        self.proxies = self.load_proxies_from_file('proxies.txt')
        Logger.info(f"Initialized scraper with {len(self.proxies)} proxies")
        self.current_proxy_index = 0
        self.session = None
        self.total_processed = 0
        self.batch_delay = batch_delay
        self.images_dir = 'images'

        # Create images directory if it doesn't exist
        os.makedirs(self.images_dir, exist_ok=True)
        Logger.info(f"Initialized image downloader")

    def load_proxies_from_file(self, file_name: str) -> list[str]:
        proxies = []
        with open(file_name, 'r') as file:
            for line in file:
                parts = line.strip().split(':')
                if len(parts) == 4:
                    proxy_url = f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}"
                    proxies.append(proxy_url)
        return proxies

    async def get_company_description(self, company_name: str, proxy_url: str) -> str | None:
        try:
            query = f"{company_name} company's description"
            encoded_query = urllib.parse.quote(query)
            search_url = f"https://search.yahoo.com/search?p={encoded_query}"

            async with self.session.get(
                    search_url,
                    proxy=proxy_url,
                    headers={
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36'
                    },
                    timeout=aiohttp.ClientTimeout(total=self.timeout)
            ) as response:
                if response.status != 200:
                    raise Exception(f"HTTP {response.status}")

                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')

                # Find all description blocks
                description_blocks = soup.select('.algo-sr .compText')

                if not description_blocks:
                    raise Exception("No description found")

                # Combine all found descriptions
                return description_blocks[0].get_text(strip=True)

        except Exception as e:
            Logger.error("Unable to fetch description", e)
            raise e

    async def download_image(self, image_url: str, proxy_url: str) -> str:
        try:
            async with self.session.get(
                    image_url,
                    timeout=aiohttp.ClientTimeout(total=self.timeout),
                    proxy=proxy_url,
                    headers={
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36'
                    }
            ) as response:
                if response.status != 200:
                    raise Exception(f"HTTP {response.status}")

                # Generate unique filename using UUID
                file_extension = image_url.split('.')[-1].split('?')[0]  # Handle URLs with query parameters
                if not file_extension or len(file_extension) > 4:
                    file_extension = 'jpg'

                filename = f"{uuid.uuid4()}.{file_extension}"
                filepath = os.path.join(self.images_dir, filename)

                # Save image
                content = await response.read()
                with open(filepath, 'wb') as f:
                    f.write(content)

                return filename
        except Exception as e:
            raise Exception(f"Error downloading image: {str(e)}")

    async def get_company_logo_url(self, company_name: str, proxy_url: str) -> Tuple[str, str]:
        try:
            query = f"{company_name} company's logo"
            encoded_query = urllib.parse.quote(query)

            search_url = f"https://www.bing.com/images/search?q={encoded_query}&first=1"

            async with self.session.get(
                    search_url,
                    proxy=proxy_url,
                    headers={
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36'
                    },
                    timeout=aiohttp.ClientTimeout(total=self.timeout)
            ) as response:
                if response.status != 200:
                    raise Exception(f"HTTP {response.status}")

                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                img_tags = soup.select('.imgpt > a[m]')

                for tag in img_tags:
                    try:
                        m_data = json.loads(tag['m'])
                        image_url = m_data.get('murl')
                        if image_url and any(ext in image_url.lower() for ext in ['.jpg', '.jpeg', '.png']):
                            try:
                                local_image_url = await self.download_image(image_url, proxy_url)
                                return local_image_url, image_url
                            except:
                                continue
                    except (json.JSONDecodeError, KeyError):
                        continue

            raise Exception("No image found")
        except Exception as e:
            Logger.error('Error fetching image', e)
            raise e

    async def fetch_url_data(self, company_name: str) -> Dict[str, str]:
        proxy_url = self.proxies[self.current_proxy_index]
        self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxies)

        company_description = await self.get_company_description(company_name, proxy_url)
        local_image_url, company_logo_url = await self.get_company_logo_url(company_name, proxy_url)

        return {
            'description': company_description,
            'local_image_url': local_image_url,
            'image_url': company_logo_url,
        }

    async def process_record(self, record: Dict[str, Any]) -> Dict[str, Any] | None:
        """
        Process a single record
        """
        try:
            record_id = record['_id']
            company_name = record['name']

            scraped_data = await self.fetch_url_data(company_name)

            # Update record
            update_data = {
                'scraped': True,
                'processed': 'processed',
                'logourl': scraped_data['image_url'],
                'description': scraped_data['description'],
                'local_image_url': scraped_data['local_image_url'],
            }
            Logger.info(f"Updating record {record_id}", update_data)

            await self.db.queue_collection.update_one(
                {'_id': record_id},
                {
                    '$set': update_data,
                    '$inc': {'retry': 1}
                }
            )

            Logger.info(f"Successfully processed record {record_id}")
            self.total_processed += 1
            return record_id

        except Exception as e:
            await self.db.queue_collection.update_one(
                {'_id': record['_id']},
                {
                    '$set': {
                        'processed': 'failed',
                    },
                    '$inc': {'retry': 1}
                }
            )

            Logger.error(f"Error processing record {record['_id']}", e)
            return None

    async def process_batch(self, batch: List[Dict[str, Any]]):
        """
        Process a batch of records concurrently using asyncio.gather
        """
        tasks = [
            self.process_record(record)
            for record in batch if record['retry'] < self.max_retries
        ]

        await asyncio.gather(*tasks, return_exceptions=True)

    async def run(self):
        """
        Main method to run the scraper
        """
        try:
            # Connect to database
            await self.db.connect()

            # Initialize aiohttp session
            self.session = aiohttp.ClientSession()

            Logger.info("Starting parallel scraping process")

            # Get total count of unscraped records
            total_unscraped = await self.db.queue_collection.count_documents({
                'scraped': False,
                'retry': {'$lt': self.max_retries}
            })

            if total_unscraped == 0:
                Logger.info("No unscraped records found")
                return

            Logger.info(f"Found {total_unscraped} unscraped records")

            # Process in batches
            batch_number = 0
            while True:
                batch_number += 1

                # Get batch of unscraped records
                batch = await self.db.queue_collection.find({
                    'scraped': False,
                    'retry': {'$lt': self.max_retries}
                }).limit(self.batch_size).to_list(length=self.batch_size)

                if not batch:
                    break

                Logger.debug(f"Starting batch {batch_number}")
                await self.process_batch(batch)

                # Calculate and log progress
                progress = (self.total_processed / total_unscraped) * 100

                Logger.info(
                    "Progress Update",
                    {
                        'batch_number': batch_number,
                        'progress_percentage': f"{progress:.2f}%",
                        'records_processed': self.total_processed,
                        'total_records': total_unscraped,
                    }
                )
                Logger.info(f'Sleeping for {self.batch_delay} seconds')
                time.sleep(self.batch_delay)

            processed = await self.db.queue_collection.count_documents({
                'scraped': True
            })

            Logger.info(f"Scraping Completed {processed} records.")

        except Exception as e:
            Logger.error("Error in scraping process", e)
        finally:
            if self.session:
                await self.session.close()
            await self.db.close()


async def main():
    scraper = ParallelScraper(
        batch_size=50,
        timeout=10,
        max_retries=5,
        batch_delay=0,
    )
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(main())
