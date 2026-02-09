from configparser import ConfigParser
from argparse import ArgumentParser

from utils.server_registration import get_cache_server
from utils.config import Config
from crawler import Crawler

from crawlerstats import print_stats

# print("Starting crawler")


def main(config_file, restart):
    # print(f"Using config file: {config_file}")
    cparser = ConfigParser()
    cparser.read(config_file)
    # print("Config file loaded successfully.")
    config = Config(cparser)
    config.cache_server = get_cache_server(config, restart)
    crawler = Crawler(config, restart)
    # print("Starting crawler")
    crawler.start()

    print_stats()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--restart", action="store_true", default=False)
    parser.add_argument("--config_file", type=str, default="config.ini")
    args = parser.parse_args()
    main(args.config_file, args.restart)

