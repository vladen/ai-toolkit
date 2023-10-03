import chromadb
import hashlib
import json
import os
import re
import sys
import tiktoken
import traceback

from chromadb.utils import embedding_functions
from datetime import datetime
from urllib.parse import urlparse, urlunparse

from const import *

encoding = tiktoken.encoding_for_model(GPT_MODEL_NAME)


def log(message):
    record = f"{datetime.now().isoformat()} | {message}"
    if sys.exc_info()[0]:
        print(f"{record}\n{traceback.format_exc()}")
    else:
        print(record)

def change_extension(path, new_extension):
    root, _ = os.path.splitext(path)
    return root + new_extension

def count_dialog_tokens(messages, tokens_per_message=3, tokens_per_name=1):
    num_tokens = 0
    for message in messages:
        num_tokens += tokens_per_message
        for key, value in message.items():
            num_tokens += len(encoding.encode(value))
            if key == "name":
                num_tokens += tokens_per_name
    num_tokens += tokens_per_message
    return num_tokens


def count_text_tokens(text):
    return len(encoding.encode(text))


def get_current_timestamp():
    return int(datetime.utcnow().timestamp() * 1000)


def handle_arg(args, arg_name, default_value=None):
    if hasattr(args, arg_name):
        return getattr(args, arg_name)
    elif not default_value:
        log(f"Argument '{arg_name}' was not provided")
    return default_value


def handle_chroma_arg(args):
    path = handle_arg(args, 'chroma')
    if path:
        try:
            chroma_client = chromadb.PersistentClient(path)
            chroma_collection = chroma_client.get_or_create_collection(
                name="documents",
                embedding_function=embedding_functions.OpenAIEmbeddingFunction(
                    api_key=os.getenv("OPENAI_API_KEY"),
                    model_name="text-embedding-ada-002"
                ))
            if not args.quiet:
                log(f"Opened Chroma DB: {str(chroma_collection.count())} documents")
            return chroma_collection
        except Exception as e:
            log(f"Failed to open Chroma DB: {str(e)}")
            return None
    else:
        return None


def handle_filter_arg(args, default_value):
    if args.filter:
        try:
            re.compile(args.filter)
            return args.filter
        except Exception:
            log(f"Argument 'filter' is not valid: {args.filter}")
            return None
    else:
        return default_value


def handle_folder_arg(args, create_if_not_exists=True):
    folder = handle_arg(args, 'folder')
    if folder:
        if not os.path.exists(folder):
            if create_if_not_exists:
                try:
                    os.makedirs(folder)
                except Exception as e:
                    log(f"Failed to create folder: {str(e)}")
                    return None
            else:
                log(f"Folder does not exists: {folder}")
                return None
        return folder
    return None


def handle_limit_arg(args, default_value):
    if args.limit:
        try:
            limit = int(args.limit)
            if limit > 0:
                return limit
        except:
            pass
        log(f"Argument 'limit' is not valid: {args.limit}")
        return None
    return default_value


def handle_url_arg(args):
    url = handle_arg(args, 'url')
    if url:
        [base_url, parsed_url] = parse_url(url)
        if parsed_url:
            return [base_url, parsed_url]
        log(f"Argument 'url' is not valid: {args.url}")
    return [None, None]


def hash_url(url):
    return hashlib.md5(url.encode()).hexdigest()


def normalise_whitespace(text):
    return re.sub(r"\s+", " ", text).strip()


def parse_url(url):
    try:
        parsed_url = urlparse(url)
        base_url = urlunparse(parsed_url._replace(fragment='', query=''))
        return [base_url, parsed_url]
    except Exception:
        log("Failed to parse URL: {url}")
    return [None, None]


def filter_links(page_links, url_filter):
    filtered_links = set()
    for link in page_links:
        [base_url, _] = parse_url(link[0])
        if base_url and url_filter.match(base_url):
            filtered_links.add(base_url)
    return filtered_links


def restore_session(target_folder, url_filter, document_limit=None, quiet=False):
    pending_urls = set()
    scraped_files = set()
    scraped_urls = set()
    file_names = [f for f in os.listdir(target_folder) if f.endswith('.json')]
    if not file_names:
        return [pending_urls, scraped_urls, scraped_files]
    if not quiet:
        log(f"Restoring session: {len(file_names)} files")
    for file_name in file_names:
        file_path = os.path.join(target_folder, file_name)
        with open(file_path, 'r') as file:
            document = json.load(file)
            if 'url' in document:
                url = document['url']
                if len(url) and url_filter.match(url):
                    scraped_files.add(file_path)
                    scraped_urls.add(url)
            for link_url in filter_links(document.get('links', []), url_filter):
                if not link_url in scraped_urls:
                    pending_urls.add(link_url)
                    if document_limit and len(pending_urls) >= document_limit:
                        break
    return [pending_urls, scraped_urls, scraped_files]
