import argparse
import json
import sys

from util import *

default_document_limit = 3

# main logic


def query_chroma_db(chroma_collection, document_limit, text, verbose):
    try:
      include = ["distances"]
      if verbose == True:
          include.append("documents")
      results = chroma_collection.query(
          include=include,
          n_results=document_limit,
          query_texts=[text],
      )
      log(f"Queried Chroma DB for: {text}\n{json.dumps(results, ensure_ascii=False, indent=2)}")
    except Exception as e:
        log(f"Failed to query Chroma DB: {str(e)}")

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
    text = args.text
    if text is None or len(text) < 1:
        log("Argument 'text' was not provided")
    # activate tool
    log(f"Ready to scrape using args:")
    log(f"\tBase url: {base_url}")
    log(f"\tDocument limit: {document_limit}")
    log(f"\tTarget folder: {target_folder}")
    chroma_collection = open_chroma_db(base_url, target_folder)
    if not chroma_collection is None:
        log(f"Opened chroma database: {str(chroma_collection.count())} documents")
    query_chroma_db(chroma_collection, document_limit, text, args.verbose)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""
          I can query Chroma DB to retrieve scraped web content.
          Just tell me the URL of initial web page, the path to the folder where extracted data is stored and the text to query the database for.
          Enjoy!
        """)
    parser.add_argument(
        "url", help="URL to the page to start scraping")
    parser.add_argument(
        "folder", help="path to the folder where scraped data will be saved")
    parser.add_argument(
        "text", help="text to search in the Chroma DB")
    parser.add_argument(
        "-l", "--limit", type=int, help=f"maximum number of results to return, {default_document_limit} by default")
    parser.add_argument(
        "-v", "--verbose", type=bool, help=f"additional logging", default=False)
    args = parser.parse_args()
    main(args)
