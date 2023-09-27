import argparse
import math
import openai
import regex
import signal
import sys
import time
import tiktoken
import threading

from prompts import *
from util import *

DOCUMENT_LIMIT = 100
GPT_MODEL_NAME = "gpt-3.5-turbo-instruct"
GPT_TEMPERATURE = 0.1
GPT_TOKEN_LIMIT = 2048

# graceful shutdown
shutdown_requested = False


def signal_handler(sig, frame):
    global shutdown_requested
    log(" ...shutting down")
    shutdown_requested = True


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# main logic


def upsert_document(chroma_collection, json_document, verbose):
    if chroma_collection is None:
        return
    summaries = []
    questions = []
    for item in json_document.get("gpt", []):
        summary = item.get("summary", "").strip()
        if len(summary) == 0:
            continue
        questions.extend(item.get("questions", []))
        summaries.append(summary)
    url = json_document["url"]
    if len(summaries) == 0:
        log(f"Ignored document, missing summaries: {url}")
        return False
    document = json.dumps({
        "summaries": summaries,
        "questions": questions,
        "items": json_document["items"],
        "links": json_document["links"],
    }, indent=3)
    metadata = {
        "updated": get_current_timestamp()
    }
    try:
        chroma_collection.upsert(
            documents=[document],
            metadatas=[metadata],
            ids=[url]
        )
        if verbose == True:
            log(f"Upserted document to chroma db: {url}\n{document}")
            return True
    except Exception as e:
        log(f"Failed to upsert document to Chroma DB: {url}\n{str(e)}")
    return False


def extract_chunks(encoding, json_document, token_limit):
    duplicates = set()
    chunks = [""]
    new_chunk = True
    simplification = regex.compile(r"\W+", regex.UNICODE, cache_pattern=True)
    title = ""
    title_done = False
    title_token_limit = token_limit / 5
    truncation = regex.compile(r"\p{P}", regex.UNICODE, cache_pattern=True)

    def truncate_text(text, token_count, token_limit):
        text_len = len(text)
        extra_length = math.ceil(
            (text_len/token_count) * (token_count - token_limit))
        if extra_length < 1:
            return text
        match = truncation.search(text, endpos=text_len - extra_length)
        if match:
            return text[:extra_length - match.end()]
        else:
            return text

    for [type, text] in json_document["items"]:
        simplified_text = simplification.sub(' ', text)
        if simplified_text in duplicates:
            continue
        else:
            duplicates.add(simplified_text)
        item = f"{type}: {text}\n"
        if title_done == False and (type in ["Title", "UncategorizedText"]):
            title += item
            token_count = len(encoding.encode(title))
            if token_count > title_token_limit:
                truncate_text(title, token_count, title_token_limit)
                title_done = True
        else:
            title_done = True
            chunk = chunks[-1] if len(chunks) > 0 else ""
            if new_chunk == True:
                chunk += title
                new_chunk = False
            chunk += item
            token_count = len(encoding.encode(chunk))
            if token_count > token_limit:
                chunks[-1] = truncate_text(chunk, token_count, token_limit)
                log(f"Truncated text:\n{chunk}")
                new_chunk = True
            else:
                chunks[-1] = chunk
    return chunks


def summarize_document(json_document, json_path, url, verbose):
    encoding = tiktoken.get_encoding("cl100k_base")
    token_count = len(encoding.encode(make_summary_prompt("")))
    log(f"Prompt template tokens: {token_count}")
    token_limit = (GPT_TOKEN_LIMIT - token_count) / 2
    chunks = extract_chunks(encoding, json_document, token_limit)
    results = []
    for chunk in chunks:
        prompt = make_summary_prompt(chunk)
        token_count = len(encoding.encode(prompt))
        if verbose == True:
            log(f"Summarizing with Chat GPT: {token_count} tokens, {url}")
        openai.api_key = os.getenv("OPENAI_API_KEY")
        try:
            response = openai.Completion.create(
                model=GPT_MODEL_NAME,
                prompt=prompt,
                max_tokens=GPT_TOKEN_LIMIT - token_count,
                temperature=GPT_TEMPERATURE
            )
            gpt_document = json.loads(response.choices[0].text)
            questions = gpt_document.get("questions", [])
            summary = gpt_document.get("summary", "")
            total_tokens = gpt_document["usage"]["total_tokens"]
            if verbose == True:
                log(f"Summarized with Chat GPT: {total_tokens} tokens, {url}")
            results.append({
                "id": response.id,
                "questions": questions,
                "summary": summary,
                "usage": response.usage
            })
        except Exception as e:
            log(
                f"Failed to summarize with Chat GPT:\n\t{json_path}\n\t{url}\n\t{str(e)}")
    return results


def upload_document(chroma_collection, json_path, verbose):
    success = False
    # try:
    with open(json_path, 'r') as file:
        json_document = json.load(file)
        items = json_document.get("items", [])
        if len(items) == 0:
            if verbose == True:
                log(f"Skipping file with no items: {json_path}")
                return False
        url = json_document["url"]
        [url, _] = parse_url(url)
        if verbose == True:
            log(f"Uploading file:\n\t{json_path}\n\t{url}")
        results = summarize_document(
            json_document, json_path, url, verbose)
        if len(results) == 0:
            return False
        json_document["gpt"] = results
        success = upsert_document(
            chroma_collection, json_document, verbose)
    if success == True:
        with open(json_path, 'w') as file:
            json.dump(json_document, file, indent=2, ensure_ascii=False)
        return True
    # except Exception as e:
        # log(f"Failed to upload file: {json_path}\n{str(e)}")
        # return False


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
    document_limit = parse_limit_arg(args, DOCUMENT_LIMIT)
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
        "-l", "--limit", type=int, help=f"maximum number of results to produce, {DOCUMENT_LIMIT} by default")
    parser.add_argument(
        "-v", "--verbose", type=bool, help=f"report activity to stdout", default=True)
    args = parser.parse_args()
    main(args)
