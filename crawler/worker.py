from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time

from urllib.parse import urlparse
from collections import Counter
from threading import Lock
from bs4 import BeautifulSoup
from tokenizer import tokenize, computeWordFrequencies
from crawlerstats import update_word_freq, unique_url, record_page_length, unique_subdomains, increment_page_count

freq_lock = Lock()
global_word_freq = Counter()

unique_pages = []
unique_pages_lock = Lock()

DELAY_TIME = 0.5
last_access_time = {}

def politeness_delay(url):
    # Maintain politeness with request delays
    domain = urlparse(url).netloc
    curr_time = time.time()

    if domain in last_access_time:
        elapsed_time = curr_time - last_access_time[domain]
        if elapsed_time < DELAY_TIME:
            time.sleep(DELAY_TIME - elapsed_time)
    last_access_time[domain] = time.time()


class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        # self.logger.info("Worker started")
        while True:
            # self.logger.info("Requesting URL from frontier")
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break
            # self.logger.info(f"Got URL: {tbd_url}")
            
            # Politeness delay
            politeness_delay(tbd_url)

            # Download page
            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            
            # Record unique url
            unique_url(tbd_url)

            # Tokenize page
            if resp and resp.status == 200:
                soup = BeautifulSoup(resp.raw_response.content, 'html.parser')
                for tag in soup(['script', 'style']):
                    tag.decompose()
                visible_text = soup.get_text(separator=" ", strip=True)

                token_list = tokenize(visible_text)
                freq_map = computeWordFrequencies(token_list)
                
                # Check for low-value pages
                # low tokens, or low unique tokens
                if len(token_list) < 100 or len(freq_map) / len(token_list) < 0.2:    
                    continue

                # Check for duplicate and near-duplicate pages
                is_duplicate = False
                curr_set = set(freq_map.keys())
                with unique_pages_lock:
                    for page_set in unique_pages:
                        # similarity = |A ∩ B| / |A ∪ B|
                        intersection = len(curr_set & page_set)
                        union = len(curr_set | page_set)
                        similarity = intersection / union
                        if similarity >= 0.85:
                            is_duplicate = True
                            break
                    if not is_duplicate:
                        unique_pages.append(curr_set)
                if is_duplicate:
                    self.frontier.mark_url_complete(tbd_url)
                    continue

                # Requirements
                record_page_length(tbd_url, sum(freq_map.values()))
                update_word_freq(freq_map)
                unique_subdomains(tbd_url)

                # with freq_lock:
                #     global_word_freq.update(freq_map)
                

                increment_page_count()

                # if increment_page_count():
                #     # self.logger.info(f"Crawled {self.config.max_pages} pages. Stopping Crawler.")
                #     break

            scraped_urls = scraper.scraper(tbd_url, resp)
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)
            # time.sleep(self.config.time_delay)
