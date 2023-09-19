# Web page scraper tool/daemon on python

A command-line tool to scrape texts from a given web page and the pages linked to it into JSON/CSV files and Chroma Database.

## Installation

Assuming that you already have `python 3.10+` and `pip` onboard...

```bash
$ pip install --user -U nltk
$ python -m nltk.downloader punkt
$ pip install --user -U unstructured
$ pip install --user -U chromadb
```

## Usage

### Scraping web site:
```bash
$ python ./scrape/scrape.py "<url>" <folder> -l 100 -f "<filter>"
```

Where:
- `<url>` - url of initial web page to scrape;
- `<folder>` - path to working folder where scraped data will be saved;
- `-l <number>` or `--limit <number>` - maximum number of web pages to scrape;
- `-f "<filter>"` or `--filter "<filter>"` - regular expression to filter out the URLs that should not be scraped.

### Querying Chroma DB:
```bash
$ python ./scrape/query.py "<url>" <folder> -l <number> -v true "<text>"
```

Where:
- `<url>` - url of the scraped web page;
- `<folder>` - path to working folder with scraped files;
- `"<text>"` - text to query Chroma DB for;
- `-l <number>` or `--limit <number>` - maximum number of documents to query,
- `-v <boolean>` or `--verbose <boolean>` - returns contents of each found document.

### Notes

If argument `filter` is not provided, this tool scrapes urls having same `schema`, `domain` and `port` as the initial url, and having no extension or `.htm`/`.html`.

The tool continues scraping session if stopped due to the reach of documents `limit` or `SIGINT` (ctrl+c), given same initial url and folder are provided to the subsequent run.

---

## Running as MacOS daemon

To run scrape script as a daemon (in background, even when your laptop sleeps),
update `scrape/local.scrape.project.plist`
  - copy template file and replace `.project.` part in the target file name with the name of your project,
  - open target file and replace `{project}` and `{user}` placeholders and with the correct values,
  - use same values for same placeholders in examples below,
  - copy the file to the daemon folder,
  - run new daemon.

### Copying file to the daemon folder

```bash
$ cp scripts/scrape/local.scrape.name.plist /Library/LaunchDaemons
```

### Running the daemon

```bash
$ sudo launchctl start local.scrape.helpx
```

### Checking daemon status

```bash
$ sudo launchctl list | grep local.scrape.{name}
```

### Checking stdout and stderr logs

```bash
$ tail -f /Users/{user}/Projects/private/ML/datasets/adobe/helpx/stdout.log
$ tail -f /Users/{user}/Projects/private/ML/datasets/adobe/helpx/stderr.log
```

### Stopping and reloading the daemon

Reloading is needed of `.plist` file was updated.

```bash
$ sudo launchctl stop local.scrape.helpx
$ sudo launchctl unload /Library/LaunchDaemons/local.scrape.{project}.plist
$ sudo launchctl load /Library/LaunchDaemons/local.scrape.{project}.plist
```

### Checking number of files in target folder

```bash
$ cd /Users/{user}/projects/datasets/{project}
$ find . -type f | wc -l
```
