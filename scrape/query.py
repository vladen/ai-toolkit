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
        results = json.dumps(chroma_collection.query(
            include=include,
            n_results=document_limit,
            query_texts=[text],
        ), ensure_ascii=False, indent=2)
        print(results)
    except Exception as e:
        log(f"Failed to query Chroma DB: {str(e)}")

# entry point


def main(args):
    # parse args
    chroma_folder = args.chroma
    text = args.text
    verbose = args.verbose
    document_limit = parse_limit_arg(args, default_document_limit)
    if document_limit is None:
        sys.exit(1)
    if text is None or len(text) < 1:
        log("Argument 'text' was not provided")
        sys.exit(1)
    # activate tool
    chroma_collection = open_chroma_db(chroma_folder, verbose)
    if verbose == True:
        log(f"Ready to send query:")
        log(f"\tDocument limit: {document_limit}")
        log(f"\tPath to Chroma DB: {chroma_folder}")
        log(f"\tText: {text}")
    query_chroma_db(chroma_collection, document_limit, text, verbose)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""
          I can query Chroma DB for scraped web content.
          Just tell me the path to the Chroma DB folder and some text to find.
          Enjoy!
        """)
    parser.add_argument(
        "chroma", help="path to Chroma DB folder")
    parser.add_argument(
        "text", help="text to search in the Chroma DB")
    parser.add_argument(
        "-l", "--limit", type=int, help=f"maximum number of results to return, {default_document_limit} by default")
    parser.add_argument(
        "-v", "--verbose", type=bool, help=f"report activity to stdout", default=False)
    args = parser.parse_args()
    main(args)
