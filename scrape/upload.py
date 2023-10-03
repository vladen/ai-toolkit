import argparse
import math
import numpy
import openai
import os
import regex
import signal
import sys
import time
import tiktoken
import threading

from const import *
from prompt import *
from util import *

DOCUMENT_LIMIT = 100

openai.api_key = os.getenv("OPENAI_API_KEY")

# graceful shutdown
shutdown_requested = False


def signal_handler(sig, frame):
    global shutdown_requested
    log(" ...shutting down")
    shutdown_requested = True


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# main logic

def count_content_tokens(content):
    encoding = tiktoken.encoding_for_model(GPT_MODEL_NAME)
    return len(encoding.encode(content))

def count_dialog_tokens(messages):
    encoding = tiktoken.encoding_for_model(GPT_MODEL_NAME)
    # https://github.com/openai/openai-cookbook/blob/main/examples/How_to_count_tokens_with_tiktoken.ipynb
    tokens_per_message = 3
    tokens_per_name = 1
    num_tokens = 0
    for message in messages:
        num_tokens += tokens_per_message
        for key, value in message.items():
            num_tokens += len(encoding.encode(value))
            if key == "name":
                num_tokens += tokens_per_name
    num_tokens += 3
    return num_tokens

def upsert_document(chroma_collection, document, quiet=False):
    target_name = document['name']
    target_url = document['url']
    texts = []
    for result in document['chunks']:
        texts.append(result["chunk"])
        texts.append("---")
        for [question, answer] in zip(result["questions"], result["answers"]):
            texts.append(question)
            texts.append(answer)
            texts.append("")
    link_texts = []
    link_urls = []
    for link in document["links"]:
        link_urls.append(link[0])
        link_texts.append(link[1] if len(link) > 1 else "")
    metadata = {
        "link_urls": "\n".join(link_urls),
        "link_texts": "\n".join(link_texts),
        "updated": datetime.now().isoformat() # TODO: use web page metadata
    }
    document = "\n".join(texts)
    try:
        if not quiet:
            log(f"Upserting document to Chroma DB: {target_url} {target_name}")
        chroma_collection.upsert(
            documents=[document],
            metadatas=[metadata],
            ids=[target_url]
        )
        return True
    except:
        log(f"Failed to upsert document to Chroma DB: {target_url} {target_name}")
    return False



def extract_chunks(text, token_limit, quiet = False):
    chunks = [""]
    new_chunk = True
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
    for line in text.split("\n"):
        if not title_done and line.startswith("Title:"):
            title += f"{line}\n"
            token_count = count_content_tokens(title)
            if token_count > title_token_limit:
                truncate_text(title, token_count, title_token_limit)
                title_done = True
        else:
            title_done = True
            chunk = chunks[-1] if len(chunks) > 0 else ""
            if new_chunk:
                chunk += title
                new_chunk = False
            chunk += f"{line}\n"
            token_count = count_content_tokens(chunk)
            if token_count > token_limit:
                chunks[-1] = truncate_text(chunk, token_count, token_limit)
                if not quiet:
                    log(f"Truncated text:\n{chunk}")
                new_chunk = True
            else:
                chunks[-1] = chunk
    return chunks


def summarize_text(target_name, target_url, text, quiet):
    token_count = count_dialog_tokens(make_summary_dialog(""))
    token_limit = (GPT_TOKEN_LIMIT - token_count) / 2
    chunks = extract_chunks(text, token_limit, quiet)
    results = []
    for chunk in chunks:
        messages = make_summary_dialog(chunk)
        token_count = count_dialog_tokens(messages)
        max_tokens = GPT_TOKEN_LIMIT - token_count
        if not quiet:
            log(f"Analysing document with Chat GPT: {target_url} {target_name}\n\t{token_count}/{max_tokens} tokens")
        try:
            response = openai.ChatCompletion.create(
                messages=messages,
                max_tokens=GPT_TOKEN_LIMIT - token_count,
                model=GPT_MODEL_NAME,
                temperature=GPT_TEMPERATURE
            )
            usage = response.get("usage", {})
            gpt_document = json.loads(response.choices[0].message.content)
            questions = gpt_document.get("questions", [])
            answers = gpt_document.get("answers", [])
            results.append({
                "id": response.id,
                "chunk": chunk,
                "questions": questions, # TODO: ensure array
                "answers": answers, # TODO: ensure array
                "usage": usage
            })
        except:
            log(f"Failed to analyse document with Chat GPT: {target_url} {target_name}")
    return results


def upload_document(chroma_collection, document_path, quiet=False):
    try:
        with open(document_path, 'r') as file:
            document = json.load(file)
        target_name = document['name']
        target_url = document['url']
        with open(change_extension(document_path, '.txt'), 'r') as file:
            text = file.read()
        results = summarize_text(target_name, target_url, text, quiet)
        if not results:
            return False
        document["chunks"] = results
        if not upsert_document(chroma_collection, document, quiet):
            return False
        with open(document_path, 'w') as file:
            if not quiet:
                log(f"Updating document: {target_url} {target_name}")
            json.dump(document, file, indent=2, ensure_ascii=False)
        return True
    except:
        log(f"Failed to upload document: {os.path.basename(document_path)}")
    return False


def upload_documents(chroma_collection, target_folder, url_filter, document_limit=DOCUMENT_LIMIT, quiet=False):
    [_, scraped_urls, scraped_files] = restore_session(
        target_folder, url_filter, document_limit, quiet)
    if not scraped_files:
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
    if not quiet:
        log(f"Uploading files: {len(pending_files)} pending")
    try:
        while not shutdown_requested and len(uploaded_files) < document_limit and len(pending_files):
            time.sleep(0.1)
            document_path = pending_files.pop()
            result = upload_document(chroma_collection, document_path, quiet)
            if result == True:
                uploaded_files.append(document_path)
            else:
                failed_files.append(document_path)
            if not quiet:
                log(f"Uploading files: {len(pending_files)} pending, {len(uploaded_files)} uploaded, {len(failed_files)} failed")
    except KeyboardInterrupt:
        pass


def main(args):
    chroma_collection = handle_chroma_arg(args)
    document_limit = handle_limit_arg(args, DOCUMENT_LIMIT)
    target_folder = handle_folder_arg(args)
    url_filter = handle_filter_arg(args, r".*")
    if not chroma_collection or not target_folder or not document_limit or not url_filter:
        sys.exit(1)
    if not args.quiet:
        log(f"Ready to upload using args:")
        log(f"  Path to target folder: {target_folder}")
        log(f"  Path to Chroma DB: {args.chroma}")
        log(f"  Url filter: {url_filter}")
        log(f"  Document limit: {document_limit}")
    thread = threading.Thread(
        target=upload_documents,
        args=(
            chroma_collection,
            target_folder,
            re.compile(url_filter),
            document_limit,
            args.quiet
        )
    )
    thread.daemon = True
    thread.start()
    thread.join()
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
        "-q", "--quiet", action="store_true", help=f"suppress logging to stdout")
    args = parser.parse_args()
    main(args)
