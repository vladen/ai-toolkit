import chromadb
import hashlib
import json
import os
import re
import sys
import traceback

from datetime import datetime
from urllib.parse import urlparse, urlunparse


def get_current_timestamp():
    return int(datetime.utcnow().timestamp() * 1000)


def hash_url(url):
    return hashlib.md5(url.encode()).hexdigest()


def log(message):
    record = f"{datetime.now().isoformat()} | {message}"
    if sys.exc_info()[0]:
        print(f"{record}\n{traceback.format_exc()}")
    else:
        print(record)


def parse_url(url):
    try:
        parsed_url = urlparse(url)
        base_url = urlunparse(parsed_url._replace(fragment='', query=''))
        return [base_url, parsed_url]
    except Exception:
        log("Failed to parse URL: {url}")
    return [None, None]


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
                name="documents")
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
            for link in document.get('links', []):
                link_url = link[0]
                [linked_url, _] = parse_url(link_url)
                if linked_url and url_filter.match(linked_url) and not link_url in scraped_urls:
                    pending_urls.add(linked_url)
                    if document_limit and len(pending_urls) == document_limit:
                        break
    pending_urls = pending_urls - scraped_urls
    return [pending_urls, scraped_urls, scraped_files]
