# accept folder and url filter
# scan json files for urls
# for each csv file
#   toss it to chatgpt
#   save summaries and questions to file (hash.txt)
#   upload summaries and texts to vector db

import argparse
import csv
import io
import openai
import pandas as pd
import signal
import sys
import time
import threading

from enum import Enum
from io import StringIO
from prompts import *
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

def create_metadata(links, url):
    metadata = {
        "links": "",
        "updated": get_current_timestamp()
    }
    for link in links:
        if len(metadata["links"]) > 0:
            metadata["links"] += "\n"
        if len(link) > 1:
            metadata["links"] += f"{link[0]} {link[1]}"
        else:
            metadata["links"] += link[0]


def upsert_document(chroma_collection, document, metadata, url, verbose):
    if chroma_collection is None:
        return
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

def deduplicate_csv(csv_document):
    input_csv_stream = StringIO(csv_document)
    df = pd.read_csv(input_csv_stream)
    columns_to_check = ["Type", "Text"]
    data_frame = df.drop_duplicates(subset=columns_to_check, keep="first")
    output_csv_stream = StringIO()
    data_frame.to_csv(output_csv_stream, index=False)
    csv_document = output_csv_stream.getvalue()
    input_csv_stream.close()
    output_csv_stream.close()
    return csv_document

def make_csv(items):
    class csv_fieldnames(str, Enum):
        type = "Type"
        text = "Text"
    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer,
        fieldnames=[member.value for member in csv_fieldnames]
    )
    writer.writeheader()
    for [type, text] in items:
        writer.writerow({
            csv_fieldnames.type: type,
            csv_fieldnames.text: text
        })
    document = buffer.getvalue()
    buffer.close()
    return deduplicate_csv(document)

def summarize_document(csv_document, json_path, url, verbose):
    prompt = make_summary_prompt(csv_document, url)
    openai.api_key = os.getenv("OPENAI_API_KEY")
    try:
        response = openai.Completion.create(
            model="gpt-3.5-turbo-instruct",
            prompt=prompt,
            max_tokens=1024,
            temperature=0.25
        )
    except Exception as e:
        log(f"Failed to summarize with Chat GPT:\n\t{len(prompt)} chars\n\t{json_path}\n\t{url}\n\t{str(e)}")
        return None
    if verbose == True:
        log(f"Summarized with Chat GPT: {url}")
    summary = response.choices[0].text if len(response.choices) > 0 else None
    return summary

def upload_document(chroma_collection, json_path, verbose):
    try:
        with open(json_path, 'r') as file:
            json_document = json.load(file)
            items = json_document.get("items", [])
            if len(items) == 0:
                if verbose == True:
                    log(f"Skipping file with no items: {json_path}")
                    return False
            links = json_document["links"]
            url = json_document["url"]
            [url, _] = parse_url(url)
            if verbose == True:
                log(f"Uploading page: {url}")
            hash = hash_url(url)
            # csv
            csv_document = make_csv(items)
            if csv_document is None:
                return False
            json_folder = os.path.dirname(json_path)
            csv_path = os.path.join(json_folder, hash + ".csv")
            with open(csv_path, "w") as file:
                file.write(csv_document)
                if verbose == True:
                    log(f"Saved csv: {csv_path}")
            # chat gpt
            summary = summarize_document(csv_document, json_path, url, verbose)
            if summary is None:
                return False
            context_document = make_context_prompt(csv_document, summary, url)
            context_path = os.path.join(json_folder, hash + ".gpt35")
            with open(context_path, "w") as file:
                file.write(context_document)
                if verbose == True:
                    log(f"Saved gpt35: {context_path}")
            # chroma db
            metadata = create_metadata(links, url)
            upsert_document(chroma_collection, context_document, metadata, url, verbose)
            return True
    except Exception as e:
        log(f"Failed to upload file: {json_path}\n{str(e)}")
        return False


def upload_documents(chroma_collection, document_limit, target_folder, url_filter, verbose):
    [_, scraped_urls, scraped_files] = restore_session(
        None, target_folder, url_filter, verbose)
    if (len(scraped_files) == 0):
        log(f"No scraped files found, you need to run scrape script first")
        sys.exit(1)
    failed_files = []
    pending_files = []
    scraped_files = list(scraped_files)
    uploaded_files = []
    for index, scraped_url in enumerate(scraped_urls):
        if url_filter.match(scraped_url):
            existing_ids = chroma_collection.get(ids=[scraped_url])["ids"]
            if len(existing_ids) == 0:
                scraped_file = scraped_files[index]
                pending_files.append(scraped_file)
    if verbose == True:
        log(f"Starting uploading files: {len(pending_files)} pending")
    try:
        while not shutdown_requested and len(uploaded_files) < document_limit and len(pending_files):
            time.sleep(0.1)
            # try to upload next pending file
            pending_file = pending_files.pop()
            result = upload_document(chroma_collection, pending_file, verbose)
            if result == True:
                uploaded_files.append(pending_file)
            else:
                failed_files.append(pending_file)
            if verbose == True:
              log(f"Uploading files: {len(pending_files)} pending, {len(uploaded_files)} uploaded, {len(failed_files)} failed")
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
    if os.getenv("OPENAI_API_KEY") is None:
        log("OPENAI_API_KEY is not set!")
        sys.exit(1)
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
