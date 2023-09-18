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


def upsert_to_chroma(chroma_collection, json_document):
    if chroma_collection is None:
        return
    document = ""
    for item in json_document["items"]:
        if "text" in item:
            document += item["text"] + "\n"
        if "links" in item:
            document += "Links:"
            for [link_text, link_url] in item["links"]:
                document += f"\n\t{link_text} {link_url}"
            document += "\n\n"
    url = json_document["url"]
    try:
        chroma_collection.upsert(
            documents=[document],
            metadatas=[{"updated": get_current_timestamp()}],
            ids=[url]
        )
    except Exception as e:
        print(f"Failed to save documents to chroma db: {str(e)}")


def clean_items(items, target_url, url_filter):
    def normalise(text):
        return re.sub(r"\s+", " ", text).strip()

    linked_urls = set()
    for item in items:
        item.pop("element_id", None)
        metadata = item.pop("metadata", {})
        text = normalise(item.pop("text", ""))
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
        links = []
        if link_urls:
            for [link_text, link_url] in zip(link_texts, link_urls):
                link_text = normalise(link_text) if link_text else ""
                link_url = urljoin(target_url, link_url, False)
                [linked_url, parsed_url] = parse_url(link_url)
                # add link to queue if it matches pattern
                if (url_filter.match(linked_url)):
                    linked_urls.add(linked_url)
                    link = [link_text, link_url]
                    links.append(link)
        if links:
            item["links"] = links
        # update item
        if metadata:
            item["metadata"] = metadata
        if len(text) > 0:
            item["text"] = text
    return [items, linked_urls]


def convert_to_csv(data):
    class csv_fieldnames(str, Enum):
        index = "Index"
        type = "Type"
        text = "Text"
    items = data.get("items", [])
    csv_buffer = io.StringIO()
    fieldnames = [member.value for member in csv_fieldnames]
    writer = csv.DictWriter(csv_buffer, fieldnames)
    writer.writeheader()
    index = 0
    for item in items:
        links = item.get("links", None)
        text = item.get("text", None)
        type = item["type"]
        writer.writerow({
            csv_fieldnames.index: index,
            csv_fieldnames.type: type,
            csv_fieldnames.text: text
        })
        index += 1
        if links:
            for [link_text, link_url] in item["links"]:
                writer.writerow({
                    csv_fieldnames.index: index,
                    csv_fieldnames.type: "Link",
                    csv_fieldnames.text: f"{link_text}: {link_url}"
                })
                index += 1
    csv_data = csv_buffer.getvalue()
    csv_buffer.close()
    return csv_data


def fetch_document(chroma_collection, target_folder, target_url, url_filter):
    # scrape the page with "unstructured"
    data = None
    try:
        log(f"Fetching page: {target_url}")
        data = elements_to_json(partition_html(url=target_url))
    except Exception as e:
        log(f"Failed to fetch {target_url}: {str(e)}")
        return None
    # reshape the data
    [items, linked_urls] = clean_items(
        json.loads(data), target_url, url_filter)
    # save as json and csv files
    hash = hash_url(target_url)
    log(f"Saved as: {hash}")
    csv_path = os.path.join(target_folder, hash + ".csv")
    json_path = os.path.join(target_folder, hash + ".json")
    json_document = {
        "url": target_url,
        "files": [csv_path, json_path],
        "items": items
    }
    upsert_to_chroma(chroma_collection, json_document)
    with open(csv_path, "w") as file:
        file.write(convert_to_csv(json_document))
    with open(json_path, "w") as file:
        json.dump(json_document, file, indent=2, ensure_ascii=False)
    return linked_urls


def restore_session(target_folder, url_filter):
    pending_urls = set()
    scraped_urls = set()
    file_names = [f for f in os.listdir(target_folder) if f.endswith('.json')]
    if not len(file_names):
        return [pending_urls, scraped_urls]
    log(f"Restoring session: {len(file_names)} files")
    for file_name in file_names:
        file_path = os.path.join(target_folder, file_name)
        with open(file_path, 'r') as f:
            json_document = json.load(f)
            if 'url' in json_document:
                url = json_document['url']
                if len(url) and url_filter.match(url):
                    scraped_urls.add(url)
            if 'items' in json_document:
                for item in json_document['items']:
                    if 'links' in item:
                        for [link_text, link_url] in item['links']:
                            [linked_url, parsed_url] = parse_url(link_url)
                            if len(linked_url) and url_filter.match(linked_url):
                                pending_urls.add(linked_url)
    pending_urls = pending_urls - scraped_urls
    return [pending_urls, scraped_urls]


def scrape_url(base_url, chroma_collection, document_limit, target_folder, url_filter):
    document_count = 0
    [pending_urls, scraped_urls] = restore_session(target_folder, url_filter)
    if (len(pending_urls) == 0):
        pending_urls.add(base_url)
    log(f"Starting scraping: {len(pending_urls)} pending, {len(scraped_urls)} scraped")
    try:
        while not shutdown_requested and document_count < document_limit and len(pending_urls):
            time.sleep(0.1)
            # try to fetch next pending url
            target_url = pending_urls.pop()
            linked_urls = fetch_document(
                chroma_collection, target_folder, target_url, url_filter)
            if linked_urls is None:
                continue
            scraped_urls.add(target_url)
            matching_urls = [
                link_url for link_url in linked_urls
                if not link_url in scraped_urls
            ]
            pending_urls.update(matching_urls)
            document_count += 1
            log(f"Scraping: {len(pending_urls)} pending, {len(scraped_urls)} scraped")
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
    url_filter = parse_filter_arg(args, parsed_url)
    if url_filter is None:
        sys.exit(1)
    # activate tool
    log(f"Ready to scrape using args:")
    log(f"\tBase url: {base_url}")
    log(f"\tDocument limit: {document_limit}")
    log(f"\tTarget folder: {target_folder}")
    log(f"\tUrl filter: {url_filter}")
    chroma_collection = open_chroma_db(base_url, target_folder)
    if not chroma_collection is None:
      log(f"Opened chroma database: {str(chroma_collection.count())} documents")
    thread = threading.Thread(
        target=scrape_url,
        args=(base_url, chroma_collection, document_limit, target_folder, re.compile(url_filter))
    )
    thread.daemon = True
    thread.start()
    thread.join()
    log("Bye!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""
          I can scrape web pages into JSON/CSV files plus Chroma DB.
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
    args = parser.parse_args()
    main(args)
