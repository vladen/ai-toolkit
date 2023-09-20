# TODO: failed pages will be re-scraped after restart because not saved as visited

import argparse
import csv
import io
import json
import os
import re
import threading
import time
import signal
import sys

from enum import Enum
from unstructured.partition.html import partition_html
from unstructured.staging.base import elements_to_json
from urllib.parse import urljoin

from util import *

default_document_limit = 10000

# graceful shutdown
shutdown_requested = False


def signal_handler(sig, frame):
    global shutdown_requested
    log(" ...shutting down")
    shutdown_requested = True


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# main logic


def clean_items(items, target_url, url_filter):
    def normalise(text):
        return re.sub(r"\s+", " ", text).strip()

    all_links = []
    linked_urls = set()
    prev_index = None
    prev_type = None
    text_length = 0
    for index, item in enumerate(items):
        item.pop("element_id", None)
        metadata = item.pop("metadata", {})
        text = normalise(item.pop("text", ""))
        type = item["type"]
        # delete unnecessary metadata
        metadata.pop("emphasized_text_contents", None)
        metadata.pop("emphasized_text_tags", None)
        metadata.pop("filetype", None)
        metadata.pop("page_number", None)
        metadata.pop("url", None)
        # reshape remaining metadata
        link_urls = metadata.pop("link_urls", [])
        link_texts = metadata.pop("link_texts", [])
        # remove links to "#"
        while "#" in link_urls:
            index = link_urls.index("#")
            link_urls.pop(index)
            link_texts.pop(index)
        # reshape links
        if link_urls:
            for [link_text, link_url] in zip(link_texts, link_urls):
                link_text = normalise(link_text) if link_text else ""
                link_url = urljoin(target_url, link_url, False)
                [linked_url, parsed_url] = parse_url(link_url)
                # add link to queue if it matches pattern
                if (len(link_url) and url_filter.match(linked_url)):
                    linked_urls.add(linked_url)
                    if len(link_text):
                        all_links.append([link_url, link_text])
                    else:
                        all_links.append([link_url])
        # update item
        if metadata:
            item["metadata"] = metadata
        if len(text) > 0:
            if len(text) > 128:
                text_length += len(text)
                item["text"] = text
            elif not prev_type is None and prev_type == type:
                prev_item = items[prev_index]
                if "text" in prev_item:
                    if prev_item["text"].find(text) == -1:
                        prev_item["text"] += f" | {text}"
                        text_length += len(text) + 3
                else:
                    prev_item["text"] = text
                    text_length += len(text)
            else:
                item["text"] = text
                text_length += len(text)
        if prev_type is None or prev_type != type:
            prev_index = index
            prev_type = type
    texful_items = [[item['type'], item['text']]
                    for item in items if "text" in item]
    return [texful_items, text_length, linked_urls, all_links]


def fetch_document(target_folder, target_url, url_filter, verbose):
    # scrape the page with "unstructured"
    data = None
    try:
        if verbose == True:
            log(f"Fetching page: {target_url}")
        data = elements_to_json(partition_html(url=target_url))
    except Exception as e:
        log(f"Failed to fetch {target_url}: {str(e)}")
        return None
    # reshape the data
    [items, text_length, linked_urls, links] = clean_items(
        json.loads(data), target_url, url_filter)
    # save as json and csv files
    hash = hash_url(target_url)
    csv_path = os.path.join(target_folder, hash + ".csv")
    json_path = os.path.join(target_folder, hash + ".json")
    json_document = {
        "url": target_url,
        "files": [csv_path, json_path],
        "length": text_length,
        "items": items,
        "links": links
    }
    with open(json_path, "w") as file:
        json.dump(json_document, file, indent=2, ensure_ascii=False)
    if verbose == True:
        log(f"Saved as: {hash}")
    return linked_urls


def scrape_url(base_url, document_limit, target_folder, url_filter, verbose):
    document_count = 0
    [pending_urls, scraped_urls, _] = restore_session(
        None, target_folder, url_filter, verbose)
    if (len(pending_urls) == 0):
        pending_urls.add(base_url)
    if verbose == True:
        log(f"Starting scraping pages: {len(pending_urls)} pending, {len(scraped_urls)} scraped")
    try:
        while not shutdown_requested and document_count < document_limit and len(pending_urls):
            time.sleep(0.1)
            # try to fetch next pending url
            target_url = pending_urls.pop()
            linked_urls = fetch_document(
                target_folder, target_url, url_filter, verbose)
            if linked_urls is None:
                continue
            scraped_urls.add(target_url)
            matching_urls = [
                link_url for link_url in linked_urls
                if not link_url in scraped_urls
            ]
            pending_urls.update(matching_urls)
            document_count += 1
            if verbose == True:
                log(f"Scraping pages: {len(pending_urls)} pending, {len(scraped_urls)} scraped")
    except KeyboardInterrupt:
        pass

# entry point


def main(args):
    # parse args
    [base_url, parsed_url] = parse_url_arg(args)
    if parsed_url is None:
        sys.exit(1)
    document_limit = parse_limit_arg(args, default_document_limit)
    if document_limit is None:
        sys.exit(1)
    target_folder = parse_folder_arg(args)
    if target_folder is None:
        sys.exit(1)
    url_filter = parse_filter_arg(args, get_default_filter(parsed_url))
    if url_filter is None:
        sys.exit(1)
    verbose = args.verbose
    # activate tool
    log(f"Ready to scrape using args:")
    log(f"\tBase url: {base_url}")
    log(f"\tDocument limit: {document_limit}")
    log(f"\tTarget folder: {target_folder}")
    log(f"\tUrl filter: {url_filter}")
    thread = threading.Thread(
        target=scrape_url,
        args=(base_url, document_limit, target_folder,
              re.compile(url_filter), verbose)
    )
    thread.daemon = True
    thread.start()
    thread.join()
    log("Bye!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""
          I can scrape web pages into JSON/CSV files.
          Just tell me the URL to the initial web page and the path to the folder where to store extracted data.
          Enjoy!
        """)
    parser.add_argument(
        "url", help="URL of the page to scrape")
    parser.add_argument(
        "folder", help="path to the folder where to save extracted data")
    parser.add_argument(
        "-f", "--filter", help="optional URL regex pattern filtering found links out, base URL of the initial page by default (schema, domain, port)")
    parser.add_argument(
        "-l", "--limit", type=int, help=f"maximum number of URLs to fetch, {default_document_limit} by default")
    parser.add_argument(
        "-v", "--verbose", type=bool, help=f"report activity to stdout", default=True)
    args = parser.parse_args()
    main(args)
