import chromadb
import hashlib
import json
import os
import re

from datetime import datetime
from urllib.parse import urlparse, urlunparse

def get_current_timestamp():
    return int(datetime.utcnow().timestamp() * 1000)


def hash_url(url):
    return hashlib.md5(url.encode()).hexdigest()


def log(message):
    print(f"{datetime.now().isoformat()} | {message}")


def open_chroma_db(path, verbose):
    try:
        chroma_client = chromadb.PersistentClient(path=path)
        chroma_collection = chroma_client.get_or_create_collection(name="scrape")
        if verbose == True:
            log(f"Opened chroma database: {str(chroma_collection.count())} documents")
        return chroma_collection
    except Exception as e:
        log(f"Failed to open chroma database: {str(e)}")
        return None


def parse_url(url):
    base_url = None
    parsed_url = None
    try:
        parsed_url = urlparse(url)
        base_url = urlunparse(parsed_url._replace(fragment='', query=''))
    except Exception:
        log("Failed to parse URL: {url}")
        parsed_url = None
    return [base_url, parsed_url]


def get_default_filter(parsed_url):
    netloc = re.escape(parsed_url.netloc)
    scheme = re.escape(parsed_url.scheme)
    return fr'^{scheme}://{netloc}($|(/.*)?/[^.]+$|/.*\.html?)'

def parse_filter_arg(args, default_filter):
    if args.filter is None:
        return default_filter
    else:
        try:
            re.compile(args.filter)
            return args.filter
        except Exception:
            log(f"Argument 'filter' id not valid: {args.filter}")
            return None


def parse_folder_arg(args, create=True):
    if args.folder is None:
      log("Argument 'folder' was not provided")
      return None
    else:
      folder = args.folder
      if create == True:
          if not os.path.exists(folder):
              try:
                  os.makedirs(folder)
              except Exception as e:
                  log(f"Failed to create target folder: {str(e)}")
                  return None
      elif not os.path.exists(folder):
          log(f"Target folder does not exists: {folder}")
          return None
      return folder


def parse_limit_arg(args, default_value):
    limit = default_value
    if not args.limit is None:
        try:
            limit = int(args.limit)
        except ValueError:
            pass
    if limit < 1:
        log(f"Argument 'limit' is not valid: {limit}")
        return None
    return limit


def parse_url_arg(args):
    base_url = None
    parsed_url = None
    if args.url is None:
        log("Argument 'url' was not provided")
    else:
        [base_url, parsed_url] = parse_url(args.url)
        if parsed_url is None:
            log(f"Argument 'url' is not valid: {args.url}")
    return [base_url, parsed_url]

def restore_session(file_limit, target_folder, url_filter, verbose):
    file_count = 0
    pending_urls = set()
    scraped_files = set()
    scraped_urls = set()
    file_names = [f for f in os.listdir(target_folder) if f.endswith('.json')]
    if not len(file_names):
        return [pending_urls, scraped_urls, scraped_files]
    if verbose == True:
        log(f"Restoring session: {len(file_names)} files")
    for file_name in file_names:
        file_path = os.path.join(target_folder, file_name)
        with open(file_path, 'r') as f:
            json_document = json.load(f)
            if 'url' in json_document:
                url = json_document['url']
                if len(url) and url_filter.match(url):
                    scraped_files.add(file_path)
                    scraped_urls.add(url)
                    file_count += 1
                    if not file_limit is None and file_count >=file_limit: break
            for link in json_document.get('links', []):
                link_url = link[0]
                [linked_url, parsed_url] = parse_url(link_url)
                if len(linked_url) and url_filter.match(linked_url):
                    pending_urls.add(linked_url)
    pending_urls = pending_urls - scraped_urls
    return [pending_urls, scraped_urls, scraped_files]