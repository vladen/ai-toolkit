import argparse
import json
import os
import re
import threading
import time
import signal
import sys

from unstructured.partition.html import partition_html
from unstructured.staging.base import elements_to_json
from urllib.parse import urljoin

from util import *

DOCUMENT_LIMIT = 10000

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
                item["text"] = text
            elif not prev_type is None and prev_type == type:
                prev_item = items[prev_index]
                if "text" in prev_item:
                    if prev_item["text"].find(text) == -1:
                        prev_item["text"] += f" | {text}"
                else:
                    prev_item["text"] = text
            else:
                item["text"] = text
        if prev_type is None or prev_type != type:
            prev_index = index
            prev_type = type
    texful_items = [[item['type'], item['text']]
                    for item in items if "text" in item]
    return [texful_items, linked_urls, all_links]


def fetch_document(target_folder, target_url, url_filter, quiet):
    hash = hash_url(target_url)
    data = None
    try:
        if not quiet:
            log(f"Fetching document: {hash} {target_url}")
        data = elements_to_json(partition_html(url=target_url))
    except:
        log(f"Failed to fetch: {target_url}")
        return None
    [items, linked_urls, links] = clean_items(
        json.loads(data), target_url, url_filter)
    json_path = os.path.join(target_folder, hash + ".json")
    json_document = {
        "url": target_url,
        "hash": hash,
        "items": items,
        "links": links
    }
    with open(json_path, "w") as file:
        json.dump(json_document, file, indent=2, ensure_ascii=False)
    return linked_urls


def scrape_url(base_url, target_folder, url_filter, document_limit=DOCUMENT_LIMIT, quiet=False):
    document_count = 0
    [pending_urls, scraped_urls, _] = restore_session(
        target_folder, url_filter, document_limit, quiet)
    if (len(pending_urls) == 0):
        pending_urls.add(base_url)
    if not quiet:
        log(f"Updated queue: {len(pending_urls)} pending, {len(scraped_urls)} scraped")
    try:
        while not shutdown_requested and document_count < document_limit and len(pending_urls):
            time.sleep(0.1)
            # try to fetch next pending url
            target_url = pending_urls.pop()
            linked_urls = fetch_document(
                target_folder, target_url, url_filter, quiet)
            scraped_urls.add(target_url)
            if not linked_urls is None:
                matching_urls = [
                    link_url for link_url in linked_urls
                    if not link_url in scraped_urls
                ]
                pending_urls.update(matching_urls)
            document_count += 1
            if not quiet:
                log(f"Updated queue: {len(pending_urls)} pending, {len(scraped_urls)} scraped")
    except KeyboardInterrupt:
        pass

# entry point


def main(args):
    # parse args
    target_folder = handle_folder_arg(args)
    document_limit = handle_limit_arg(args, DOCUMENT_LIMIT)
    [base_url, parsed_url] = handle_url_arg(args)
    if parsed_url:
        netloc = re.escape(parsed_url.netloc)
        scheme = re.escape(parsed_url.scheme)
        default_filter = fr"^{scheme}://{netloc}(?:[^/]+/)*[^.]+(?:\.html?)?$"
        url_filter = handle_filter_arg(args, default_filter)
    if not document_limit or not parsed_url or not target_folder or not url_filter:
        sys.exit(1)
    log(f"Ready to scrape web pages using args:")
    log(f"\t Page URL: {base_url}")
    log(f"\t Target folder: {target_folder}")
    log(f"\t URL filter: {url_filter}")
    log(f"\t Document limit: {document_limit}")
    thread = threading.Thread(
        target=scrape_url,
        args=(base_url, target_folder, re.compile(
            url_filter), document_limit, args.quiet)
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
        "url", help="URL of the initial page to scrape")
    parser.add_argument(
        "folder", help="path to the data folder")
    parser.add_argument(
        "-f", "--filter", help="optional regex filtering links found on scrapped pages, by default - base URL of the initial page")
    parser.add_argument(
        "-l", "--limit", type=int, help=f"maximum number of URLs to fetch, by default - {DOCUMENT_LIMIT}")
    parser.add_argument(
        "-q", "--quiet", action="store_true", help=f"suppress logging to stdout")
    args = parser.parse_args()
    main(args)
