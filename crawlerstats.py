from threading import Lock
from collections import defaultdict, Counter
from urllib.parse import urlparse, urldefrag


stats_lock = Lock()

# page_lock = Lock()
# STOP_CRAWL = False

pages_crawled = 0
MAX_PAGES_TO_CRAWL = 25 # Testing

# Other Requirements
# 1. Number of unique URLs
unique_urls = set()

# 2. Longest page (by word count)
longest_page = None
longest_page_length = 0

# 3. Total word frequency map
word_freq = Counter()

# 4. Number of subdomains
subdomains = defaultdict(int)


def increment_page_count():
    global pages_crawled, STOP_CRAWL
    with stats_lock:
        pages_crawled += 1
        # if pages_crawled % 100 == 0:
        #     print(f"---Crawled {pages_crawled} pages---")
        return pages_crawled >= MAX_PAGES_TO_CRAWL

def unique_url(url):
    cleaned_url, _ = urldefrag(url)
    with stats_lock:
        unique_urls.add(cleaned_url)


def record_page_length(url, word_count):
    global longest_page, longest_page_length
    with stats_lock:
        if word_count > longest_page_length:
            longest_page = url
            longest_page_length = word_count


STOP_WORDS = {
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and",
    "any", "are", "aren't", "as", "at", "be", "because", "been", "before", "being",
    "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't",
    "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during",
    "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't",
    "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here",
    "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i",
    "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's",
    "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself",
    "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought",
    "our", "ours", "ourselves", "out", "over", "own", "same", "shan't", "she",
    "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such",
    "than", "that", "that's", "the", "their", "theirs", "them", "themselves",
    "then", "there", "there's", "these", "they", "they'd", "they'll", "they're",
    "they've", "this", "those", "through", "to", "too", "under", "until", "up",
    "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were",
    "weren't", "what", "what's", "when", "when's", "where", "where's", "which",
    "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would",
    "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours",
    "yourself", "yourselves",
    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p",
    "q", "r", "s", "t", "u", "v", "w", "x", "y", "z", 
    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
    "00", "01", "02", "03", "04", "05", "06", "07", "08", "09",
    "10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
    "20", "30", "40", "50", "60", "70", "80", "90",
}

def update_word_freq(curr_freq):
    filtered = {word: count for word, count in curr_freq.items() if word not in STOP_WORDS}
    with stats_lock:
        for word, count in filtered.items():
            word_freq[word] += count


def unique_subdomains(url):
    parsed = urlparse(url)
    host = parsed.netloc.lower()

    if host.endswith(".uci.edu"):
        with stats_lock:
            subdomains[host] += 1

def print_stats():
    print("### Statistics ###")
    print(f"Unique URLs: {len(unique_urls)}")
    print(f"Longest page: {longest_page}")
    print(f"Longest page word count: {longest_page_length}")

    print(f"Top 150 words:")
    for word, count in word_freq.most_common(150):
        print(f"{word}: {count}")

    print(f"Subdomains: {len(subdomains)}")
    # Alphabetical order
    for subdomain in sorted(subdomains.keys()):
        print(f"{subdomain}: {subdomains[subdomain]}")
    # for subdomain, count in subdomains.items():
    #     print(f"{subdomain}: {count}")