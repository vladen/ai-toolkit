import chromadb
import hashlib
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


def open_chroma_db(base_url, target_folder):
    try:
        file_name = f"_chroma.{str(hash_url(base_url))}"
        chroma_path = os.path.join(target_folder, file_name)
        chroma_client = chromadb.PersistentClient(path=chroma_path)
        return chroma_client.get_or_create_collection(name="scrape")
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


def parse_filter_arg(args, parsed_url):
    if args.filter is None:
        netloc = re.escape(parsed_url.netloc)
        scheme = re.escape(parsed_url.scheme)
        return fr'^{scheme}://{netloc}($|(/.*)?/[^.]+$|/.*\.html?)'
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
