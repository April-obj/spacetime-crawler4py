import os
import shelve

from threading import Thread, RLock
import time
from urllib.parse import urlparse
from queue import Queue, Empty

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config

        self.lock = RLock()
        self.domain_last_access = {}
        self.politeness_delay = 0.5

        # Queue implementation
        #self.to_be_downloaded = list()
        self.to_be_downloaded = Queue()
        
        if not os.path.exists(self.config.save_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
        # Load existing save file, or create one if it does not exist.
        # self.save = shelve.open(self.config.save_file, flag='c', protocol=None, writeback=False)
        self.db_lock = RLock()
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            with shelve.open(self.config.save_file) as save:
                if not save:
                    for url in self.config.seed_urls:
                        self.add_url(url)

    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        # total_count = len(self.save)
        total_count = 0
        tbd_count = 0
        with shelve.open(self.config.save_file) as save:
            total_count = len(save)
            for url, completed in save.values():
                if not completed and is_valid(url):
                    self.to_be_downloaded.put(url)
                    tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def get_tbd_url(self):
        try:
            url = self.to_be_downloaded.get_nowait()
        except Empty:
            return None
        
        domain = urlparse(url).netloc
        

        with self.lock:     
            now = time.time()
            last_access = self.domain_last_access.get(domain, 0)

            wait_time = self.politeness_delay - (now - last_access)

            if wait_time > 0:
                time.sleep(wait_time)
            
            self.domain_last_access[domain] = time.time()

        return url

    def add_url(self, url):

        url = normalize(url)
        urlhash = get_urlhash(url)

        # Thread-safe
        with self.db_lock:
            with shelve.open(self.config.save_file) as save:
                if urlhash not in save:
                    save[urlhash] = (url, False)
                    # self.save.sync()
                    self.to_be_downloaded.put(url)
    
    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)

        # Thread-safe
        with self.db_lock:
            with shelve.open(self.config.save_file) as save:
                if urlhash not in save:
                    # This should not happen.
                    self.logger.error(
                        f"Completed url {url}, but have not seen it before.")

                save[urlhash] = (url, True)
                # self.save.sync()
