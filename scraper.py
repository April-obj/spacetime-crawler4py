import re
from urllib.parse import urlparse, urljoin

from bs4 import BeautifulSoup
import urllib.robotparser

robot_cache = None

def get_robot_cache():
    global robot_cache
    if robot_cache is None:
        robot_cache = RobotParserCache()
    return robot_cache

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content

    # List for urls
    url_list = []

    # check reponse status
    if resp.status != 200:
        # invalid response, return empty list
        return url_list

    # Parse content
    try:
        soup = BeautifulSoup(resp.raw_response.content, 'html.parser')
        for link in soup.find_all('a', href=True):
            href = link['href']
        
            if not href or href.strip() == '':
                continue

            abs_url = urljoin(url, href)
            
            # Add to list
            url_list.append(abs_url)
    except Exception as e:
        # print(f"Parsing Error in {url}: {e}")
        return []
        

    # set to remove duplicates
    return list(dict.fromkeys(url_list))


def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        # Normalize URL
        url = normalize_url(url)
        parsed = urlparse(url)

        if parsed.scheme not in set(["http", "https"]):
            return False
        
        # Domain check
        allowed_domains = [
            'ics.uci.edu', 'cs.uci.edu', 'informatics.uci.edu',
            'stat.uci.edu']
        
        if not any(parsed.netloc.lower().endswith(domain) for domain in allowed_domains):
            return False

        # Trap checks
        # Login pages
        login_patterns = ["login", "sign_in", "signin", "log_in", "auth"]
        for pattern in login_patterns:
            if pattern in parsed.path.lower():
                return False
            
        # Calendar pages
        if re.search(r"/(calendar|events?)/", parsed.path.lower()):
            return False
        if re.search(r"/\d{4}/\d{1,2}/\d{1,2}/", parsed.path.lower()):
            return False
        
        # Dynamic URL patterns
        dynamic_patterns = [
            r'cfid=', r'cid=', r'sessionid=', r'session=', r'ssid=',
            r'ref=', r'replytocom=', r'utm_', r'fbclid=', r'gclid=',
            r'sid=', r'view=', r'sort=', r'order=', r'page=', r'filter=',
            r'cftoken=', r'jsessionid=']
        
        for pattern in dynamic_patterns:
            if re.search(pattern, parsed.query.lower()):
                return False

        # Bad url types
        bad_url_patterns = [
            r'/cgi-bin/', r'/wp-admin/', r'/wp-content/', r'/administrator/', r'/phpmyadmin/',
            r'server-status', r'/.git/', r'/.svn/', r'/.env'
        ]
        for pattern in bad_url_patterns:
            if re.search(pattern, parsed.path.lower()):
                return False
            
        # Too many parameters
        if parsed.query and len(parsed.query.split('&')) > 10:
            return False

        # File extensions
        if re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower()): 
            return False

        # Check robots.txt
        if not get_robot_cache().robots_allowed(url):
            return False
        
        return True

    except TypeError:
        # print ("TypeError for ", parsed)
        return False
    except ValueError:
        # print ("ValueError for ", parsed)
        return False


class RobotParserCache:
    def __init__(self, timeout=5):
        self.cache = {}
        self.timeout = timeout

    def robots_allowed(self, url, user_agent="CS121Crawler"):
        # Check robots.txt for url
        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        robots_url = f"{base_url}/robots.txt"

        if base_url not in self.cache:
            rp = urllib.robotparser.RobotFileParser()

            try:
                rp.set_url(robots_url)
                with urllib.request.urlopen(robots_url, timeout=self.timeout) as response:
                    content = response.read()
                    rp.parse(content.decode('utf-8').splitlines())
                self.cache[base_url] = rp
            except Exception as e:
                print(f"Error checking robots.txt in {url}: {e}")
                self.cache[base_url] = None
        
        rp = self.cache.get(base_url)

        if rp:
            return rp.can_fetch(user_agent, url)
        return True


def normalize_url(url):
    parsed = urlparse(url)
    
    if parsed.port == 80 or parsed.port == 443:
        netloc = parsed.hostname
    else:
        netloc = parsed.netloc
    
    # Remove fragment
    normalized = parsed._replace(netloc=netloc, fragment='')

    scheme = normalized.scheme.lower()
    netloc = netloc.lower()

    query_params = parsed.query.split('&')
    if query_params and query_params != ['']:
        query_params.sort()
        query = '&'.join(query_params)
    else:
        query = ""
    
    # Remove trailing slash
    path = parsed.path.rstrip('/')
    if not path:
        path = '/'
    
    normalized = f"{scheme}://{netloc}{path}?{query}" if query else f"{scheme}://{netloc}{path}"
    return normalized



    

