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


def fetch_url(target_url, target_name, quiet):
    list_index = 0
    page_links = []
    page_texts = []
    try:
        if not quiet:
            log(f"Fetching url: {target_url} {target_name}")
        elements = partition_html(url=target_url)
        for element in elements:
            if not hasattr(element, "text") or not element.text:
                continue
            if element.category == "ListItem":
                lines = [line for line in str(
                    element.text).splitlines() if line.strip()]
                if lines:
                    if list_index == 0:
                        page_texts.append("List:")
                    list_index += 1
                    page_texts.append(f"{list_index}. {normalise_whitespace(lines[0])}")
                    for line in lines[1:]:
                        page_texts.append(f"- {normalise_whitespace(line)}")
            else:
                list_index = 0
                page_texts.append(
                    f"{element.category}: {normalise_whitespace(element.text)}")
            metadata = element.metadata
            if not hasattr(metadata, "link_urls") or not metadata.link_urls:
                continue
            for [link_text, link_url] in zip(metadata.link_texts, metadata.link_urls):
                if link_url:
                    link_url = urljoin(target_url, link_url, False)
                    if link_text:
                        page_links.append(
                            [link_url, normalise_whitespace(link_text)])
                    else:
                        page_links.append([link_url])
        if not len(page_texts):
            log(f"Fetched page has no texts: {target_url} {target_name}\n{elements_to_json(elements, indent=2)}")
            return None, None
        return page_links, page_texts
    except:
        log(f"Failed to fetch url: {target_url} {target_name}")
        return None, None


def scrape_url(base_url, target_folder, url_filter, document_limit=DOCUMENT_LIMIT, quiet=False):
    document_count = 0
    [pending_urls, scraped_urls, _] = restore_session(
        target_folder, url_filter, document_limit, quiet)
    if (len(pending_urls) == 0):
        pending_urls.add(base_url)
    if not quiet:
        log(f"Scraping pages: {len(pending_urls)} pending, {len(scraped_urls)} scraped")
    try:
        while not shutdown_requested and document_count < document_limit and len(pending_urls):
            time.sleep(0.1)
            target_url = pending_urls.pop()
            target_url_hash = hash_url(target_url)
            page_links, page_texts = fetch_url(
                target_url, target_url_hash, quiet)
            if page_texts is None:
                continue
            base_path = os.path.join(target_folder, target_url_hash)
            with open(base_path + ".txt", "w", encoding="utf-8") as file:
                file.write("\n".join(page_texts))
            with open(base_path + ".json", "w", encoding="utf-8") as file:
                json.dump({
                    "url": target_url,
                    "name": target_url_hash,
                    "links": page_links
                }, file, ensure_ascii=False, indent=2)
            document_count += 1
            scraped_urls.add(target_url)
            pending_urls.update(filter_links(page_links, url_filter))
            pending_urls -= scraped_urls
            if not quiet:
                log(f"Scraping pages: {len(pending_urls)} pending, {len(scraped_urls)} scraped")
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
    log(f"  Initial page URL: {base_url}")
    log(f"  Path to target folder: {target_folder}")
    log(f"  URL filter: {url_filter}")
    log(f"  Document limit: {document_limit}")
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
