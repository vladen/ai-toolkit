import argparse
import sys

from util import *

DOCUMENT_LIMIT = 3

# main logic


def query_chroma_db(chroma_collection, query_text, document_limit=DOCUMENT_LIMIT, quiet=True):
    try:
        include = ["distances"]
        if quiet == False:
            include.append("documents")
            include.append("metadatas")
        results = chroma_collection.query(
            include=include,
            n_results=document_limit,
            query_texts=[query_text],
        )
        if not results["ids"]:
            return
        for index, ids in enumerate(results["ids"]):
            if not ids:
                continue
            id = ids[0]
            if quiet == False:
                distance = results["distances"][index][0]
                document = results["documents"][index][0]
                metadatas = results["metadatas"][index][0]
                links = len(metadatas.get("link_urls", "").split("\n"))
                updated = metadatas.get("updated")
                print(f"""
Distance: {distance}
Url: {id}

Document:
{document}

Metadatas:
links: {links}
updated: {updated}
                """)
            else:
                print(id)
    except:
        log("Failed to query Chroma DB")

# entry point


def main(args):
    chroma_collection = handle_chroma_arg(args)
    if not chroma_collection:
        sys.exit(1)
    document_limit = handle_limit_arg(args, DOCUMENT_LIMIT)
    if not document_limit:
        sys.exit(1)
    if not args.text:
        log("Argument 'text' was not provided")
        sys.exit(1)
    if not args.quiet:
        log(f"Ready to query data:")
        log(f"\t Path to Chroma DB: {args.chroma}")
        log(f"\t Query text: {args.text}")
        log(f"\t Document limit: {document_limit}")
    query_chroma_db(chroma_collection, args.text, document_limit, args.quiet)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""
          I can query Chroma DB for scraped web content.
          Just tell me the path to the Chroma DB folder and some text to find.
          Enjoy!
        """)
    parser.add_argument(
        "chroma", help="path to the Chroma DB folder")
    parser.add_argument(
        "text", help="text to search in the Chroma DB")
    parser.add_argument(
        "-l", "--limit", type=int, help=f"maximum number of documents to return, by default - {DOCUMENT_LIMIT}")
    parser.add_argument(
        "-q", "--quiet", action="store_true", help=f"suppress logging to stdout")
    args = parser.parse_args()
    main(args)
