import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
import os
import re
import sys
from collections import deque
import traceback
from concurrent.futures import ThreadPoolExecutor

user_input = "Starting URL:"
BASE_URL = user_input.split("fandom.com")[0] + "fandom.com"
START_PATH = user_input.split("fandom.com")[1]
DUMP_DIR = './dump'

MAX_CONCURRENT_WORKERS = os.cpu_count() // 3
REQUEST_LIMIT = 5
REQUEST_TIMEOUT = 30
USER_AGENT = 'FandomWikiArchiver/1.0 (Purpose: personal offline use)'

os.makedirs(DUMP_DIR, exist_ok=True)

class URLManager:
    def __init__(self, start_url):
        self.urls_to_visit = deque([start_url])
        self.visited_urls = {start_url}
        self._lock = asyncio.Lock()

    async def get_next_url(self):
        async with self._lock:
            if self.urls_to_visit:
                return self.urls_to_visit.popleft()
            return None

    async def add_new_urls(self, urls):
        async with self._lock:
            new_urls_added = 0
            for url in urls:
                if url not in self.visited_urls:
                    self.visited_urls.add(url)
                    self.urls_to_visit.append(url)
                    new_urls_added += 1
            return new_urls_added

    def get_queue_size(self):
        return len(self.urls_to_visit)

    def get_visited_count(self):
        return len(self.visited_urls)

class WikiScraper:
    def __init__(self, base_url, dump_dir, user_agent, executor):
        self.base_url = base_url
        self.dump_dir = dump_dir
        self.user_agent = user_agent
        self.robot_parser = RobotFileParser()
        self.executor = executor

    async def setup_robots_parser(self):
        robots_url = urljoin(self.base_url, '/robots.txt')
        print(f"Fetching robots.txt from: {robots_url}")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(robots_url, headers={'User-Agent': self.user_agent}) as response:
                    if response.status == 200:
                        lines = (await response.text()).splitlines()
                        self.robot_parser.parse(lines)
                        print("Successfully fetched and parsed robots.txt")
                    else:
                        print(f"Warning: Could not fetch robots.txt (Status: {response.status}). Proceeding without checks.", file=sys.stderr)
        except Exception as e:
            print(f"Warning: Could not fetch or parse robots.txt: {e}", file=sys.stderr)
            print("Proceeding without robots.txt checks - Be extra cautious!", file=sys.stderr)

    def is_valid_url(self, url):
        parsed = urlparse(url)
        if parsed.netloc != urlparse(self.base_url).netloc:
            return False
        if not parsed.path.startswith('/wiki/'):
            return False
        if any(prefix in parsed.path for prefix in [
            '/wiki/Special:', '/wiki/File:', '/wiki/Category:', '/wiki/User:',
            '/wiki/User_blog:', '/wiki/Template:', '/wiki/Help:', '/wiki/MediaWiki:',
            '/wiki/Talk:', '/wiki/Forum:', '/wiki/Board:'
        ]):
            return False
        if 'action=edit' in parsed.query or 'action=history' in parsed.query:
            return False
        return True

    def url_to_filename(self, url):
        parsed = urlparse(url)
        path = parsed.path.replace('/wiki/', '', 1).replace('/', '_')
        filename = re.sub(r'[\\/*?:"<>|]', '_', path) or 'index'
        return os.path.join(self.dump_dir, f"{filename[:200]}.txt")

    async def fetch_page(self, session, url):
        if not self.robot_parser.can_fetch(self.user_agent, url):
            return None
        try:
            async with session.get(url, headers={'User-Agent': self.user_agent}, timeout=REQUEST_TIMEOUT) as response:
                response.raise_for_status()
                return await response.text()
        except aiohttp.ClientError as e:
            print(f"Error fetching {url}: {e}", file=sys.stderr)
        except asyncio.TimeoutError:
            print(f"Timeout error fetching {url}", file=sys.stderr)
        except Exception as e:
            print(f"An unexpected error occurred fetching {url}: {e}", file=sys.stderr)
        return None

    def _parse_and_save(self, html_content, current_url):

        soup = BeautifulSoup(html_content, 'html.parser')
        content_div = soup.find('div', class_='mw-parser-output') or soup.find('div', id='mw-content-text')
        if not content_div:
            return None, []

        for tag in content_div.find_all(['script', 'style', 'nav', 'table.infobox', 'div.toc', 'figure', 'aside']):
            if tag:
                tag.decompose()

        text = content_div.get_text(separator='\n', strip=True)
        text = re.sub(r'\n\s*\n', '\n', text)
        text = re.sub(r' +', ' ', text)

        filename = self.url_to_filename(current_url)
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(text)
            print(f"Successfully saved text to: {os.path.basename(filename)}")
        except IOError as e:
            print(f"Error saving file {filename}: {e}", file=sys.stderr)

        new_links = []
        for link in content_div.find_all('a', href=True):
            absolute_url = urljoin(current_url, link['href'])
            cleaned_url = urlparse(absolute_url)._replace(fragment="").geturl()
            if self.is_valid_url(cleaned_url):
                new_links.append(cleaned_url)

        return text, new_links

    async def process_page(self, session, url):
        html = await self.fetch_page(session, url)
        if not html:
            return 0

        loop = asyncio.get_running_loop()
        try:
            _, new_links = await loop.run_in_executor(
                self.executor, self._parse_and_save, html, url
            )
            return new_links
        except Exception as e:
            print(f"Error during {url}: {e}", file=sys.stderr)
            traceback.print_exc()
            return []


async def worker(name, url_manager, scraper, session, semaphore):
    while True:
        current_url = await url_manager.get_next_url()
        if current_url is None:
            print(f"Worker {name}: No more URLs to process.")
            break

        async with semaphore:
            new_links = await scraper.process_page(session, current_url)
            if new_links:
                added_count = await url_manager.add_new_urls(new_links)
                if added_count > 0:
                    print(f"Worker {name}: Found and added {added_count} new URLs to the queue.")

async def main():
    start_time = asyncio.get_event_loop().time()
    start_url = urljoin(BASE_URL, START_PATH)

    url_manager = URLManager(start_url)
    with ThreadPoolExecutor(max_workers=os.cpu_count() or 4) as executor:
        scraper = WikiScraper(BASE_URL, DUMP_DIR, USER_AGENT, executor)
        await scraper.setup_robots_parser()

        semaphore = asyncio.Semaphore(REQUEST_LIMIT)
        
        async with aiohttp.ClientSession() as session:
            print(f"Starting {MAX_CONCURRENT_WORKERS} workers")
            print(f"Text files are saved in: {os.path.abspath(DUMP_DIR)}")
            print(f"Start URL: {start_url}")

            tasks = [
                worker(f"Worker ID: {i+1}", url_manager, scraper, session, semaphore)
                for i in range(MAX_CONCURRENT_WORKERS)
            ]

            await asyncio.gather(*tasks)

    end_time = asyncio.get_event_loop().time()
    print("\n--- Crawling Finished ---")
    print(f"Total unique URLs visited: {url_manager.get_visited_count()}")
    print(f"Total execution time: {end_time - start_time:.2f} seconds")
    print(f"Text files saved in: {os.path.abspath(DUMP_DIR)}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Exit")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        traceback.print_exc()
