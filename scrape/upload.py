# accept folder and url filter
# scan json files for urls
# for each csv file
#   toss it to chatgpt
#   save summaries and questions to file (hash.txt)
#   upload summaries and texts to vector db

import argparse
import signal
import sys
import time
import threading

from pathlib import Path
from util import *

default_document_limit = 100

# graceful shutdown
shutdown_requested = False


def signal_handler(sig, frame):
    global shutdown_requested
    log(" ...shutting down")
    shutdown_requested = True


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# main logic


def upsert_document(chroma_collection, items, links, url, verbose):
    if chroma_collection is None:
        return
    document = ""
    metadata = {
        "links": "",
        "updated": get_current_timestamp()
    }
    for item in items:
        if len(document) > 0:
            document += "\n"
        document += f"{item[0]}: {item[1]}"
    links = ""
    for link in links:
        if len(metadata["links"]) > 0:
            metadata["links"] += "\n"
        if len(link) > 1:
            metadata["links"] += f"{link[0]} {link[1]}"
        else:
            metadata["links"] += link[0]
    try:
        chroma_collection.upsert(
            documents=[document],
            metadatas=[metadata],
            ids=[url]
        )
        if verbose == True:
          log(f"Upserted document to chroma db: {url}")
    except Exception as e:
        log(f"Failed to upsert document to chroma db: {url}\n{str(e)}")


def upload_document(chroma_collection, json_path, verbose):
    try:
        with open(json_path, 'r') as file:
            json_document = json.load(file)
            items = json_document["items"]
            links = json_document["links"]
            url = json_document["url"]
            [url, _] = parse_url(url)
            log(f"Uploading page: {url}")
            upsert_document(chroma_collection, items, links, url, verbose)
    except Exception as e:
        log(f"Failed to upload file: {json_path}\n{str(e)}")
        return None


def upload_documents(chroma_collection, document_limit, target_folder, url_filter, verbose):
    [_, scraped_urls, scraped_files] = restore_session(
        None, target_folder, url_filter, verbose)
    if (len(scraped_files) == 0):
        log(f"No scraped files found, you need to run scrape script first")
        sys.exit(1)
    pending_files = []
    scraped_files = list(scraped_files)
    uploaded_files = set()
    for index, scraped_url in enumerate(scraped_urls):
        if url_filter.match(scraped_url):
            existing_ids = chroma_collection.get(ids=[scraped_url])["ids"]
            if len(existing_ids) == 0:
                scraped_file = scraped_files[index]
                pending_files.append(scraped_file)
    if verbose == True:
        log(f"Starting uploading files: {len(pending_files)} pending, {len(uploaded_files)} uploaded")
    try:
        while not shutdown_requested and len(uploaded_files) < document_limit and len(pending_files):
            time.sleep(0.1)
            # try to upload next pending file
            pending_file = pending_files.pop()
            upload_document(chroma_collection, pending_file, verbose)
            uploaded_files.add(pending_file)
            if verbose == True:
              log(f"Uploading files: {len(pending_files)} pending, {len(uploaded_files)} uploaded")
    except KeyboardInterrupt:
        pass


def main(args):
    # parse args
    scraped_folder = parse_folder_arg(args)
    if scraped_folder is None:
        sys.exit(1)
    document_limit = parse_limit_arg(args, default_document_limit)
    if document_limit is None:
        sys.exit(1)
    url_filter = parse_filter_arg(args, r".*")
    if url_filter is None:
        sys.exit(1)
    verbose = args.verbose
    # report args
    if verbose == True:
        log(f"Ready to upload using args:")
        log(f"\tChroma DB: {args.chroma}")
        log(f"\tDocument limit: {document_limit}")
        log(f"\tScraped folder: {scraped_folder}")
        log(f"\tUrl filter: {url_filter}")
    # connect to Chroma DB
    chroma_collection = open_chroma_db(args.chroma, verbose)
    if chroma_collection is None:
        sys.exit(1)
    # activate tool
    thread = threading.Thread(
        target=upload_documents,
        args=(
            chroma_collection,
            document_limit,
            scraped_folder,
            re.compile(url_filter),
            verbose
        )
    )
    thread.daemon = True
    thread.start()
    thread.join()
    # finalize
    log("Bye!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""
          I can use Chat GPT to summarize scraped web content and upload summaries to Chroma DB.
          Just tell me path to the folder where scraped data is stored and path to Chroma DB folder.
          Your OPENAI_API_KEY needs to be set in env vars to talk to Chat GPT.
          Enjoy!
        """)
    parser.add_argument(
        "folder", help="path to the folder where scraped data are saved")
    parser.add_argument(
        "chroma", help="path to the Chroma DB file")
    parser.add_argument(
        "-f", "--filter", help="optional regex pattern filtering scraped pages to upload by URLs")
    parser.add_argument(
        "-l", "--limit", type=int, help=f"maximum number of results to produce, {default_document_limit} by default")
    parser.add_argument(
        "-v", "--verbose", type=bool, help=f"report activity to stdout", default=True)
    args = parser.parse_args()
    main(args)
